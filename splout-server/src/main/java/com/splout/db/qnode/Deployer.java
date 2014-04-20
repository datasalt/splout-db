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
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.splout.db.common.JSONSerDe;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.transport.TTransportException;

import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.IMap;
import com.splout.db.common.PartitionEntry;
import com.splout.db.common.ReplicationEntry;
import com.splout.db.common.Tablespace;
import com.splout.db.hazelcast.CoordinationStructures;
import com.splout.db.hazelcast.TablespaceVersion;
import com.splout.db.qnode.beans.DeployInfo;
import com.splout.db.qnode.beans.DeployRequest;
import com.splout.db.qnode.beans.DeployStatus;
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
	 * Runnable that deals with the asynchronous part of the deployment. Particularly, it waits until DNodes finish their
	 * work, and then performs the version switch.
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
    private long dnodesSpreadMetadataTimeout;
		private boolean isReplicaBalancingEnabled;

		public ManageDeploy(List<String> dnodes, List<DeployRequest> deployRequests, long version,
		    long timeoutSeconds, long secondsToCheckFailureOrTimeout, long dnodesSpreadMetadataTimeout, boolean isReplicaBalancingEnabled) {
			this.dnodes = dnodes;
			this.deployRequests = deployRequests;
			this.version = version;
			this.timeoutSeconds = timeoutSeconds;
			this.secondsToCheckFailureOrTimeout = secondsToCheckFailureOrTimeout;
      this.dnodesSpreadMetadataTimeout = Math.max(dnodesSpreadMetadataTimeout, 1);
			this.isReplicaBalancingEnabled = isReplicaBalancingEnabled;
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
							abortDeploy(dnodes, "One or more DNodes failed", version);
							return;
						}
						// Let's see if we reached the timeout.
						// Negative timeoutSeconds => waits forever
						if(waitSeconds > timeoutSeconds && timeoutSeconds >= 0) {
							log.warn("Deploy of version [" + version + "] timed out. Reached [" + waitSeconds
							    + "] seconds.");
							abortDeploy(dnodes, "Timeout reached", version);
							return;
						}
					}
				} while(!finished);

				// It's still possible that the deploy failed so let's check it again
				if(checkForFailure()) {
					explainErrors();
					abortDeploy(dnodes, "One or more DNodes failed.", version);
					return;
				}

				// Check after the wait than the complete tablespaces are available to that QNode. If that is the
				// case for this QNode it will be probably the case for the rest of QNodes.
        long millisToWait = 50;
        double acumulatedMillis = 0.;
				List<SwitchVersionRequest> versionsToCheck = switchActions();
				do {
					Thread.sleep(millisToWait);
          acumulatedMillis += millisToWait;

          // Let's see if we reached the timeout.
          // Negative timeoutSeconds => waits forever
          if((acumulatedMillis/1000) > dnodesSpreadMetadataTimeout) {
            log.warn("Deploy of version [" + version + "] timed out when waiting DNodes to spread the metadata. Reached [" + (acumulatedMillis/1000)
                + "] seconds.");
            abortDeploy(dnodes, "Timeout reached", version);
            return;
          }

					Iterator<SwitchVersionRequest> it = versionsToCheck.iterator();
					while(it.hasNext()) {
						SwitchVersionRequest req = it.next();
						Tablespace t = context.getTablespaceVersionsMap().get(
						    new TablespaceVersion(req.getTablespace(), req.getVersion()));
						// Check that this TablespaceVersion has been reported by some node through Hazelcast
						if(t != null && t.getReplicationMap() != null && t.getPartitionMap() != null
						    && t.getPartitionMap().getPartitionEntries() != null
						    && t.getReplicationMap().getReplicationEntries() != null
						    && t.getReplicationMap().getReplicationEntries().size() > 0) {
							if(t.getPartitionMap().getPartitionEntries().size() == t.getReplicationMap()
							    .getReplicationEntries().size()) {
								log.info("Ok, TablespaceVersion [" + req.getTablespace() + ", " + req.getVersion()
								    + "] being handled by enough DNodes as reported by Hazelcast. ("
								    + t.getReplicationMap().getReplicationEntries() + ")");
								it.remove();
							}
						}
					}
				} while(versionsToCheck.size() > 0);

				log.info("All DNodes performed the deploy of version [" + version
				    + "]. Publishing tablespaces...");

				// We finish by publishing the versions table with the new versions.
				try {
					switchVersions(switchActions());
				} catch(UnexistingVersion e) {
					throw new RuntimeException(
					    "Unexisting version after deploying this version. Sounds like a bug.", e);
				}

				// If some replicas are under-replicated, start a balancing process
				context.maybeBalance();

				log.info("Deploy of version [" + version + "] Finished PROPERLY. :-)");
				context.getCoordinationStructures().logDeployMessage(version,
				    "Deploy of version [" + version + "] finished properly.");
				context.getCoordinationStructures().getDeploymentsStatusPanel()
				    .put(version, DeployStatus.FINISHED);
			} catch(InterruptedException e) {
				log.error("Error while deploying version [" + version + "]", e);
				abortDeploy(dnodes, e.getMessage(), version);
			} catch(Throwable t) {
				t.printStackTrace();
				throw new RuntimeException(t);
			} finally {
				CoordinationStructures.DEPLOY_IN_PROGRESS.decrementAndGet();
			}
		}

		/**
		 * Compose the list of switch actions to switch
		 * 
		 * @return
		 */
		private List<SwitchVersionRequest> switchActions() {
			ArrayList<SwitchVersionRequest> actions = new ArrayList<SwitchVersionRequest>();
			for(DeployRequest req : deployRequests) {
				actions.add(new SwitchVersionRequest(req.getTablespace(), version));
			}
			return actions;
		}

		/**
		 * Log DNodes errors in deployment. We log both to the QNode logger and to Hazelcast so the info is persisted in the
		 * session.
		 */
		private void explainErrors() {
			IMap<String, String> deployErrorPanel = context.getCoordinationStructures().getDeployErrorPanel(
			    version);
			String msg = "Deployment of version [" + version + "] failed in DNode[";
			for(Entry<String, String> entry : deployErrorPanel.entrySet()) {
				String fMsg = msg + entry.getKey() + "] - it failed with the error [" + entry.getValue() + "]";
				log.error(fMsg);
				context.getCoordinationStructures().logDeployMessage(version, fMsg);
			}
		}

		/**
		 * Return true if one or more of the DNodes reported an error.
		 */
		private boolean checkForFailure() {
			IMap<String, String> deployErrorPanel = context.getCoordinationStructures().getDeployErrorPanel(
			    version);
			if(!isReplicaBalancingEnabled) {
				return !deployErrorPanel.isEmpty();
			}
			// If replica balancing is enabled we check whether we could survive after the failed DNodes
			Set<String> failedDNodes = new HashSet<String>(deployErrorPanel.keySet());
			// Check if deploy needs to be canceled or if the system could auto-rebalance itself afterwards
			for(DeployRequest deployRequest : deployRequests) {
				for(ReplicationEntry repEntry : deployRequest.getReplicationMap()) {
					if(failedDNodes.containsAll(repEntry.getNodes())) {
						// There is AT LEAST one partition that depends on the failed DNodes so the deploy must fail!
						return true;
					}
				}
			}
			return false;
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
	 * Call this method for starting an asynchronous deployment given a proper deploy request - proxy method for
	 * {@link QNodeHandler}. Returns a {@link QueryStatus} with the status of the request.
	 * 
	 * @throws InterruptedException
	 */
	public DeployInfo deploy(List<DeployRequest> deployRequests) throws InterruptedException {
		DeployInfo deployInfo = new DeployInfo();

		// A new unique version number is generated.
		long version = context.getCoordinationStructures().uniqueVersionId();
		deployInfo.setVersion(version);

		List<String> tablespaces = new ArrayList<String>();
		List<String> dataURIs = new ArrayList<String>();

		for(DeployRequest request : deployRequests) {
			tablespaces.add(request.getTablespace());
			dataURIs.add(request.getData_uri());
		}

		deployInfo.setTablespacesDeployed(tablespaces);
		deployInfo.setDataURIs(dataURIs);

		Date startTime = new Date();
		deployInfo.setStartedAt(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(startTime));

		context.getCoordinationStructures().logDeployMessage(version,
		    "Deploy [" + version + "] for tablespaces " + tablespaces + " started.");
		context.getCoordinationStructures().getDeploymentsStatusPanel().put(version, DeployStatus.ONGOING);

		// Generate the list of actions per DNode
		Map<String, List<DeployAction>> actionsPerDNode = generateDeployActionsPerDNode(deployRequests,
		    version);

		// Starting the countdown latch.
		ICountDownLatch countDownLatchForDeploy = context.getCoordinationStructures()
		    .getCountDownLatchForDeploy(version);
		Set<String> dnodesInvolved = actionsPerDNode.keySet();
		countDownLatchForDeploy.trySetCount(dnodesInvolved.size());

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
				client.deploy(actionPerDNode.getValue(), version);
			} catch(Exception e) {
				String errorMsg = "Error sending deploy actions to DNode [" + actionPerDNode.getKey() + "]";
				log.error(errorMsg, e);
				abortDeploy(new ArrayList<String>(actionsPerDNode.keySet()), errorMsg, version);
				deployInfo.setError("Error connecting to DNode " + actionPerDNode.getKey());
				context.getCoordinationStructures().getDeployInfoPanel().put(version, deployInfo);
				return deployInfo;
			} finally {
				context.returnDNodeClientToPool(actionPerDNode.getKey(), client, renew);
			}
		}

		// Initiating an asynchronous process to manage the deployment
		deployThread.execute(new ManageDeploy(new ArrayList(actionsPerDNode.keySet()), deployRequests,
		    version, context.getConfig().getLong(QNodeProperties.DEPLOY_TIMEOUT, -1), context.getConfig()
		        .getLong(QNodeProperties.DEPLOY_SECONDS_TO_CHECK_ERROR),
        context.getConfig().getLong(QNodeProperties.DEPLOY_DNODES_SPREAD_METADATA_TIMEOUT, 180),
        context.getConfig().getBoolean(QNodeProperties.REPLICA_BALANCE_ENABLE)));

		context.getCoordinationStructures().getDeployInfoPanel().put(version, deployInfo);
		return deployInfo;
	}

	/**
	 * DNodes are informed to stop the deployment, as something failed.
	 * 
	 * @throws InterruptedException
	 */
	public void abortDeploy(List<String> dnodes, String deployerErrorMessage, long version) {
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
		context.getCoordinationStructures().logDeployMessage(version,
		    "Deploy failed due to: " + deployerErrorMessage);
		context.getCoordinationStructures().getDeploymentsStatusPanel().put(version, DeployStatus.FAILED);
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
				for(PartitionEntry partEntry : req.getPartitionMap()) {
					if(partEntry.getShard() == rEntry.getShard()) {
            if (rEntry.getShard() == 128) {
              log.error("partEntry: " + partEntry.getShard() + " rEntry: " + rEntry.getShard());
            }
						pEntry = partEntry;
					}
				}
				if(pEntry == null) {
          String msg = "No Partition metadata for shard: " + rEntry.getShard()
              + " this is very likely to be a software bug.";
          log.error(msg);
          try {
            log.error("Partition map: " + JSONSerDe.ser(req.getPartitionMap()));
            log.error("Replication map: " + JSONSerDe.ser(req.getReplicationMap()));
          } catch (JSONSerDe.JSONSerDeException e) {
            log.error("JSON error", e);
          }
          throw new RuntimeException(msg);
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
					metadata.setMinKey(pEntry.getMin());
					metadata.setMaxKey(pEntry.getMax());
					metadata.setNReplicas(rEntry.getNodes().size());
					metadata.setDeploymentDate(deployDate);
					metadata.setInitStatements(req.getInitStatements());
					metadata.setEngineId(req.getEngine());

					deployAction.setMetadata(metadata);
					actionsSoFar.add(deployAction);
				}
			}
		}
		return actions;
	}
}