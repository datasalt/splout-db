package com.splout.db.qnode;

/*
 * #%L
 * Splout SQL Server
 * %%
 * Copyright (C) 2012 Datasalt Systems S.L.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.transport.TTransportException;

import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.IMap;
import com.hazelcast.core.InstanceDestroyedException;
import com.hazelcast.core.MemberLeftException;
import com.splout.db.common.PartitionEntry;
import com.splout.db.common.ReplicationEntry;
import com.splout.db.hazelcast.CoordinationStructures;
import com.splout.db.hazelcast.TablespaceVersion;
import com.splout.db.qnode.beans.DeployInfo;
import com.splout.db.qnode.beans.DeployRequest;
import com.splout.db.qnode.beans.QueryStatus;
import com.splout.db.qnode.beans.SwitchVersionRequest;
import com.splout.db.thrift.DNodeService;
import com.splout.db.thrift.DeployAction;
import com.splout.db.thrift.PartitionMetadata;

/**
 * The Deployer is a specialized module ({@link com.splout.db.qnode.QNodeHandlerModule}) of the
 * {@link com.splout.db.qnode.QNode} that performs the business logic associated with a distributed deployment. It is
 * used by the {@link com.splout.db.qnode.QNodeHandler}.
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class Deployer extends QNodeHandlerModule {

	private final static Log log = LogFactory.getLog(Deployer.class);
	private ExecutorService deployThread;

	@SuppressWarnings("serial")
	public static class UnexistingVersion extends Exception {

		public UnexistingVersion() {
			super();
		}

		public UnexistingVersion(String message) {
			super(message);
		}
	}

	/**
	 * Runnable that can deal with the asynchronous part of the deployment. Particularly, it waits until DNodes finish
	 * their work, and then perform the version switch.
	 * <p>
	 * Currently we also use this class for synchronous deployments (empty tablespace creation) for reusing the finalizing
	 * code (publishing the version to Hazelcast). For that we don't run this in a Thread but call finalizeDeploy().
	 */
	public class ManageDeploy implements Runnable {

		// Number of seconds to wait until another
		// check to see if timeout was reached or
		// if a DNode failed.
		private long secondsToCheckFailureOrTimeout = 60l;

		private long version;
		private List<String> dnodes;
		private long timeoutSeconds;
		private List<DeployRequest> deployRequests;

		public ManageDeploy(List<String> dnodes, List<DeployRequest> deployRequests, long version,
		    long timeoutSeconds, long secondsToCheckFailureOrTimeout) {
			this.dnodes = dnodes;
			this.deployRequests = deployRequests;
			this.version = version;
			this.timeoutSeconds = timeoutSeconds;
			this.secondsToCheckFailureOrTimeout = secondsToCheckFailureOrTimeout;
		}

		@Override
		public void run() {
			log.info(context.getConfig().getProperty(QNodeProperties.PORT) + " Executing deploy for version ["
			    + version + "]");
			CoordinationStructures.DEPLOY_IN_PROGRESS.incrementAndGet();

			try {
				long waitSeconds = 0;
				ICountDownLatch countDownLatchForDeploy = context.getCoordinationStructures()
				    .getCountDownLatchForDeploy(version);
				boolean finished;
				do {
					finished = countDownLatchForDeploy.await(secondsToCheckFailureOrTimeout, TimeUnit.SECONDS);
					waitSeconds += secondsToCheckFailureOrTimeout;
					if(!finished) {
						// If any of the DNodes failed, then we cancel the deployment.
						if(checkForFailure()) {
							explainErrors();
							abortDeploy(dnodes, version);
							return;
						}
						// Let's see if we reached the timeout.
						// Negative timeoutSeconds => waits forever
						if(waitSeconds > timeoutSeconds && timeoutSeconds >= 0) {
							log.warn("Deploy of version [" + version + "] timed out. Reached [" + waitSeconds
							    + "] seconds.");
							abortDeploy(dnodes, version);
							return;
						}
					}
				} while(!finished);

				finalizeDeploy();

				CoordinationStructures.DEPLOY_IN_PROGRESS.decrementAndGet();
			} catch(MemberLeftException e) {
				log.error("Error while deploying version [" + version + "]", e);
				abortDeploy(dnodes, version);
			} catch(InstanceDestroyedException e) {
				log.error("Error while deploying version [" + version + "]", e);
				abortDeploy(dnodes, version);
			} catch(InterruptedException e) {
				log.error("Error while deploying version [" + version + "]", e);
				abortDeploy(dnodes, version);
			} catch(Throwable t) {
				t.printStackTrace();
				throw new RuntimeException(t);
			}
		}

		/**
		 * Actions that are to be performed after all DNodes finished properly a deploy. Use this method directly without
		 * running this Thread if the deploy is synchronous (empty tablespace creation).
		 */
		public void finalizeDeploy() throws InterruptedException {
			log.info("All DNodes performed the deploy of version [" + version + "]. Publishing tablespaces...");

			// We finish by publishing the versions table with the new versions.
			try {
				switchVersions(switchActions());
			} catch(UnexistingVersion e) {
				throw new RuntimeException(
				    "Unexisting version after deploying this version. Sounds like a bug.", e);
			}

			log.info("Deploy of version [" + version + "] Finished PROPERLY. :-)");

			// After a deploy we must synchronize tablespace versions to see if we have to remove some.
			context.synchronizeTablespaceVersions();
		}

		/**
		 * Compose the list of switch actions to switch
		 */
		private List<SwitchVersionRequest> switchActions() {
			ArrayList<SwitchVersionRequest> actions = new ArrayList<SwitchVersionRequest>();
			for(DeployRequest req : deployRequests) {
				actions.add(new SwitchVersionRequest(req.getTablespace(), version));
			}
			return actions;
		}

		/**
		 * Log DNodes errors in deployment
		 */
		private void explainErrors() {
			IMap<String, String> deployErrorPanel = context.getCoordinationStructures().getDeployErrorPanel(
			    version);
			String msg = "Deployment of version [" + version + "] failed because DNode[";
			for(Entry<String, String> entry : deployErrorPanel.entrySet()) {
				log.error(msg + entry.getKey() + "] failed with the error [" + entry.getValue() + "]");
			}
		}

		/**
		 * Return true if one or more of the DNodes reported an error.
		 */
		private boolean checkForFailure() {
			IMap<String, String> deployErrorPanel = context.getCoordinationStructures().getDeployErrorPanel(
			    version);
			return !deployErrorPanel.isEmpty();
		}
	} /* End ManageDeploy */

	/**
	 * The Deployer deals with deploy and switch version requests.
	 */
	public Deployer(QNodeHandlerContext context) {
		super(context);
		deployThread = Executors.newFixedThreadPool(1);
	}

	/**
	 * Attempts to send the actions per DNode for the deploy to each of the DNodes. A version must have been assigned for
	 * this deploy. If one of the DNodes fail, the process should be cancelled so an error is returned in the DeployInfo
	 * bean. If there is no error in the bean, everything went fine.
	 */
	private DeployInfo sendDeployActionsPerDNode(boolean onlyCreate, Map<String, List<DeployAction>> actionsPerDNode,
	    long version) {
		DeployInfo errDeployInfo = new DeployInfo();
		// Sending deploy signals to each DNode
		for(Map.Entry<String, List<DeployAction>> actionPerDNode : actionsPerDNode.entrySet()) {
			DNodeService.Client client = null;
			boolean renew = false;
			try {
				try {
					client = context.getDNodeClientFromPool(actionPerDNode.getKey());
				} catch(TTransportException e) {
					renew = true;
					throw e;
				}
				if(onlyCreate) {
					client.createTablespacePartitions(actionPerDNode.getValue(), version);
				} else {
					client.deploy(actionPerDNode.getValue(), version);					
				}
			} catch(Exception e) {
				log.error("Error sending deploy actions to DNode [" + actionPerDNode.getKey() + "]", e);
				errDeployInfo.setError("Error connecting to DNode " + actionPerDNode.getKey());
				return errDeployInfo;
			} finally {
				context.returnDNodeClientToPool(actionPerDNode.getKey(), client, renew);
			}
		}
		return errDeployInfo;
	}

	/**
	 * A particular case of deploy where we only create the structures needed for populating a tablespace with random
	 * inserts. This method is isolated on purpose since it is much more simple than the asynchronous deploy. This one can
	 * be executed synchronously.
	 */
	public DeployInfo createEmptyTablespace(DeployRequest deployRequest) {
		// A new unique version number is generated.
		long version = context.getCoordinationStructures().uniqueVersionId();

		// convert to a list since our API methods accept a list
		List<DeployRequest> deployRequests = Arrays.asList(new DeployRequest[] { deployRequest });
		// Generate the list of actions per DNode
		Map<String, List<DeployAction>> actionsPerDNode = generateDeployActionsPerDNode(deployRequests,
		    version);

		DeployInfo deployInfo = sendDeployActionsPerDNode(true, actionsPerDNode, version);
		if(deployInfo.getError() != null) {
			// some DNode failed -> we must cancel
			return deployInfo;
		}

		// We will use ManageDeploy in a synchronous way
		ManageDeploy manage = new ManageDeploy(new ArrayList(actionsPerDNode.keySet()), deployRequests,
		    version, -1l, -1l);
		// instead of running it we use it for executing the finalization tasks (promoting the version to Hazelcast, etc)
		try {
			manage.finalizeDeploy();
		} catch(InterruptedException e) {
			log.error("Error promoting new empty tablespace", e);
			DeployInfo errDeployInfo = new DeployInfo();
			errDeployInfo.setError("Error promoting tablespace: " + e.getMessage());
			return errDeployInfo;
		}

		deployInfo.setVersion(version);
		// actually started = end in this case, because this is synchronous
		deployInfo.setStartedAt(SimpleDateFormat.getInstance().format(new Date()));
		return deployInfo;
	}

	/**
	 * Call this method for starting an asynchronous deployment given a proper deploy request - proxy method for
	 * {@link QNodeHandler}. Returns a {@link QueryStatus} with the status of the request.
	 * 
	 * @throws InterruptedException
	 */
	public DeployInfo deploy(List<DeployRequest> deployRequests) throws InterruptedException {

		// A new unique version number is generated.
		long version = context.getCoordinationStructures().uniqueVersionId();

		// Generate the list of actions per DNode
		Map<String, List<DeployAction>> actionsPerDNode = generateDeployActionsPerDNode(deployRequests,
		    version);

		// Starting the countdown latch.
		ICountDownLatch countDownLatchForDeploy = context.getCoordinationStructures()
		    .getCountDownLatchForDeploy(version);
		Set<String> dnodesInvolved = actionsPerDNode.keySet();
		countDownLatchForDeploy.setCount(dnodesInvolved.size());

		DeployInfo deployInfo = sendDeployActionsPerDNode(false, actionsPerDNode, version);
		if(deployInfo.getError() != null) {
			// some DNode failed -> we must cancel
			abortDeploy(new ArrayList<String>(actionsPerDNode.keySet()), version);
			return deployInfo;
		}

		// Initiating an asynchronous process to manage the deployment
		deployThread.execute(new ManageDeploy(new ArrayList(actionsPerDNode.keySet()), deployRequests,
		    version, context.getConfig().getLong(QNodeProperties.DEPLOY_TIMEOUT, -1), context.getConfig()
		        .getLong(QNodeProperties.DEPLOY_SECONDS_TO_CHECK_ERROR)));

		deployInfo.setVersion(version);
		deployInfo.setStartedAt(SimpleDateFormat.getInstance().format(new Date()));
		return deployInfo;
	}

	/**
	 * DNodes are informed to stop the deployment, as something failed.
	 * 
	 * @throws InterruptedException
	 */
	public void abortDeploy(List<String> dnodes, long version) {
		for(String dnode : dnodes) {
			DNodeService.Client client = null;
			boolean renew = false;
			try {
				try {
					client = context.getDNodeClientFromPool(dnode);
				} catch(TTransportException e) {
					renew = true;
					throw e;
				}
				client.abortDeploy(version);
			} catch(Exception e) {
				log.error("Error sending abort deploy flag to DNode [" + dnode + "]", e);
			} finally {
				if(client != null) {
					context.returnDNodeClientToPool(dnode, client, renew);
				}
			}
		}
		CoordinationStructures.DEPLOY_IN_PROGRESS.decrementAndGet();
	}

	/**
	 * Switches current versions being served for some tablespaces, in an atomic way.
	 */
	public void switchVersions(List<SwitchVersionRequest> switchRequest) throws UnexistingVersion {
		// We compute the new versions table, and then try to update it
		// We use optimistic locking: we read the original
		// map and try to update it. If the original has changed during
		// this process, we retry: reload the original map, ...
		Map<String, Long> versionsTable;
		Map<String, Long> newVersionsTable;
		do {
			versionsTable = context.getCoordinationStructures().getCopyVersionsBeingServed();
			newVersionsTable = new HashMap<String, Long>();
			if(versionsTable != null) {
				newVersionsTable.putAll(versionsTable);
			}

			for(SwitchVersionRequest req : switchRequest) {
				TablespaceVersion tsv = new TablespaceVersion(req.getTablespace(), req.getVersion());
				newVersionsTable.put(tsv.getTablespace(), tsv.getVersion());
			}
		} while(!context.getCoordinationStructures().updateVersionsBeingServed(versionsTable,
		    newVersionsTable));
	}

	/**
	 * Generates the list of individual deploy actions that has to be sent to each DNode.
	 */
	private static Map<String, List<DeployAction>> generateDeployActionsPerDNode(
	    List<DeployRequest> deployRequests, long version) {
		HashMap<String, List<DeployAction>> actions = new HashMap<String, List<DeployAction>>();

		long deployDate = System.currentTimeMillis(); // Here is where we decide the data of the deployment for all deployed
		                                              // tablespaces

		for(DeployRequest req : deployRequests) {
			for(Object obj : req.getReplicationMap()) {
				ReplicationEntry rEntry = (ReplicationEntry) obj;
				PartitionEntry pEntry = null;
				// partition map can be null in which case queries will be redirected as key % partition
				if(req.getPartitionMap() != null) {
					for(PartitionEntry partEntry : req.getPartitionMap()) {
						if(partEntry.getShard() == rEntry.getShard()) {
							pEntry = partEntry;
						}
					}
					if(pEntry == null) {
						throw new RuntimeException("No Partition metadata for shard: " + rEntry.getShard()
						    + " this is very likely to be a software bug.");
					}
				}
				// Normalize DNode ids -> The convention is that DNodes are identified by host:port . So we need to strip the
				// protocol, if any
				for(int i = 0; i < rEntry.getNodes().size(); i++) {
					String dnodeId = rEntry.getNodes().get(i);
					if(dnodeId.startsWith("tcp://")) {
						dnodeId = dnodeId.substring("tcp://".length(), dnodeId.length());
					}
					rEntry.getNodes().set(i, dnodeId);
				}
				for(String dNode : rEntry.getNodes()) {
					List<DeployAction> actionsSoFar = (List<DeployAction>) MapUtils.getObject(actions, dNode,
					    new ArrayList<DeployAction>());
					actions.put(dNode, actionsSoFar);
					DeployAction deployAction = new DeployAction();
					deployAction.setDataURI(req.getData_uri() + "/" + rEntry.getShard() + ".db");
					deployAction.setTablespace(req.getTablespace());
					deployAction.setVersion(version);
					deployAction.setPartition(rEntry.getShard());

					// Add partition metadata to the deploy action for DNodes to save it
					PartitionMetadata metadata = new PartitionMetadata();
					if(pEntry != null) {
						metadata.setMinKey(pEntry.getMin());
						metadata.setMaxKey(pEntry.getMax());
					}
					if(req.getPartitionMap() == null) {
						metadata.setHasNoPartitionMap(true);
					}
					metadata.setNReplicas(rEntry.getNodes().size());
					metadata.setDeploymentDate(deployDate);
					metadata.setInitStatements(req.getInitStatements());

					deployAction.setMetadata(metadata);
					actionsSoFar.add(deployAction);
				}
			}
		}
		return actions;
	}
}