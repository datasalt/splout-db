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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.transport.TTransportException;
import org.codehaus.jackson.type.TypeReference;

import com.google.common.base.Joiner;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.splout.db.common.JSONSerDe;
import com.splout.db.common.JSONSerDe.JSONSerDeException;
import com.splout.db.common.SploutConfiguration;
import com.splout.db.common.Tablespace;
import com.splout.db.dnode.beans.DNodeSystemStatus;
import com.splout.db.hazelcast.CoordinationStructures;
import com.splout.db.hazelcast.DNodeInfo;
import com.splout.db.hazelcast.DistributedRegistry;
import com.splout.db.hazelcast.HazelcastConfigBuilder;
import com.splout.db.hazelcast.HazelcastProperties;
import com.splout.db.hazelcast.TablespaceVersion;
import com.splout.db.hazelcast.TablespaceVersionStore;
import com.splout.db.qnode.Deployer.UnexistingVersion;
import com.splout.db.qnode.QNodeHandlerContext.DNodeEvent;
import com.splout.db.qnode.QNodeHandlerContext.TablespaceVersionInfoException;
import com.splout.db.qnode.Querier.QuerierException;
import com.splout.db.qnode.beans.DeployInfo;
import com.splout.db.qnode.beans.DeployRequest;
import com.splout.db.qnode.beans.ErrorQueryStatus;
import com.splout.db.qnode.beans.QNodeStatus;
import com.splout.db.qnode.beans.QueryStatus;
import com.splout.db.qnode.beans.StatusMessage;
import com.splout.db.qnode.beans.SwitchVersionRequest;
import com.splout.db.thrift.DNodeService;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;

/**
 * Implements the business logic for the {@link QNode}.
 * <p>
 * The QNode is the most complex and delicate part of Splout. Among its responsabilities are:
 * <ul>
 * <li>Handling deploys asynchronously: One QNode will lead a deployment. It will put a flag in ZooKeeper and trigger an
 * asynchronous deploy to all involved DNodes. Then, it has to finalize the deploy properly when all DNodes are ready.
 * This is handled by the {@link Deployer} module.</li>
 * <li>Performing queries and multiqueries. This is handled by the {@link Querier} module.</li>
 * <li>Handling rollbacks: Rollbacks are easy since we just need to change the version in ZooKeeper (DNodes already have
 * the data for past version in disk). The number of versions that are saved in the system per tablespace can be
 * configured (see {@link QNodeProperties}).</li>
 * </ul>
 * For convenience, there is some in-memory state grabbed from ZooKeeper in {@link QNodeHandlerContext}. This state is
 * passed through all modules ({@link Deployer} and such). Care has to be taken to have consistent in-memory state, for
 * that it is important to handle ZooKeeper events properly and be notified always on the paths that we are interested
 * in.
 * <p>
 * One of the important business logic parts of this class is to synchronize the versions in ZooKeeper. Because we only
 * want to keep a certain amount of versions, the QNodes have to check for this and remove stalled versions if needed.
 * Then, DNodes will receive a notification and they will be able to delete the old data from disk.
 * <p>
 * The QNode returns JSON strings for all of its methods. The beans that are serialized are indicated in the
 * documentation.
 */
public class QNodeHandler implements IQNodeHandler {

	/**
	 * The JSON type reference for deserializing Multi-query results
	 */
	public final static TypeReference<ArrayList<QueryStatus>> MULTIQUERY_TYPE_REF = new TypeReference<ArrayList<QueryStatus>>() {
	};

	private final static Log log = LogFactory.getLog(QNodeHandler.class);
	private QNodeHandlerContext context;
	private Deployer deployer;
	private Querier querier;
	private SploutConfiguration config;
	private CoordinationStructures coord;

	// This flag is set to "false" after WARMING_TIME seconds (qnode.warming.time)
	// Some actions will only be taken after warming time, just in case some nodes still didn't join the cluster.
	private final AtomicBoolean isWarming = new AtomicBoolean(true);
	private Thread warmingThread;

	private final Counter meterQueriesServed = Metrics.newCounter(QNodeHandler.class, "queries-served");
	private final Meter meterRequestsPerSecond = Metrics.newMeter(QNodeHandler.class, "queries-second",
	    "queries-second", TimeUnit.SECONDS);
	private final Histogram meterResultSize = Metrics.newHistogram(QNodeHandler.class, "response-size");

	/**
	 * Keep track of die/alive DNodes events.
	 */
	public class DNodesListener implements EntryListener<String, DNodeInfo> {

		@Override
		public void entryAdded(EntryEvent<String, DNodeInfo> event) {
			log.info("DNode [" + event.getValue() + "] joins the cluster as ready to server requests.");
			// Update TablespaceVersions
			try {
				String dnode = event.getValue().getAddress();
				log.info(Thread.currentThread().getName() + " : populating client queue for [" + dnode
				    + "] as it connected.");
				context.initializeThriftClientCacheFor(dnode);
				context.updateTablespaceVersions(event.getValue(), QNodeHandlerContext.DNodeEvent.ENTRY);
				log.info(Thread.currentThread() + ": Maybe balance (entryAdded)");
				maybeBalance();
			} catch(TablespaceVersionInfoException e) {
				throw new RuntimeException(e);
			} catch(TTransportException e) {
				throw new RuntimeException(e);
			} catch(InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void entryRemoved(EntryEvent<String, DNodeInfo> event) {
			log.info("DNode [" + event.getValue() + "] left.");
			// Update TablespaceVersions
			try {
				context.discardThriftClientCacheFor(event.getValue().getAddress());
				context.updateTablespaceVersions(event.getValue(), QNodeHandlerContext.DNodeEvent.LEAVE);
				log.info(Thread.currentThread() + ": Maybe balance (entryRemoved)");
				maybeBalance();
			} catch(TablespaceVersionInfoException e) {
				throw new RuntimeException(e);
			} catch(InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

		private void maybeBalance() {
			// do this only after warming
			if(!isWarming.get()) {
				// check if we could balance some partitions
				List<ReplicaBalancer.BalanceAction> balanceActions = context.getBalanceActions();
				// we will only re-balance versions being served
				// otherwise strange things may happen: to re-balance a version in the middle of its deployment...
				Map<String, Long> versionsBeingServed = coord.getCopyVersionsBeingServed();
				for(ReplicaBalancer.BalanceAction action : balanceActions) {
					if(versionsBeingServed != null && versionsBeingServed.get(action.getTablespace()) != null
					    && versionsBeingServed.get(action.getTablespace()) == action.getVersion()) {
						// put if absent + TTL
						coord.getDNodeReplicaBalanceActionsSet().putIfAbsent(action, "",
						    config.getLong(QNodeProperties.BALANCE_ACTIONS_TTL), TimeUnit.SECONDS);
					}
				}
			}
		}

		@Override
		public void entryUpdated(EntryEvent<String, DNodeInfo> event) {
			// Update TablespaceVersions
			try {
				context.updateTablespaceVersions(event.getValue(), QNodeHandlerContext.DNodeEvent.UPDATE);
			} catch(TablespaceVersionInfoException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void entryEvicted(EntryEvent<String, DNodeInfo> event) {
			// Never happens
			log.error("Event entryEvicted received for [" + event + "]. "
			    + "Should have never happened... Something wrong in the code");
		}
	}

	public class VersionListener implements EntryListener<String, Map<String, Long>> {

		private void check(EntryEvent<String, Map<String, Long>> event) {
			if(!CoordinationStructures.KEY_FOR_VERSIONS_BEING_SERVED.equals(event.getKey())) {
				throw new RuntimeException("Unexpected key " + event.getKey() + " for map "
				    + CoordinationStructures.KEY_FOR_VERSIONS_BEING_SERVED);
			}
		}

		private void processAddOrUpdate(EntryEvent<String, Map<String, Long>> event) {
			check(event);
			try {
				// We perform all changes together with the aim of atomicity
				updateLocalTablespace(event.getValue());
			} catch(IOException e) {
				log.error(
				    "Error changing serving tablespace [" + event.getKey() + " to version [" + event.getValue()
				        + "]. Probably the system is now unstable.", e);
			}
		}

		@Override
		public void entryAdded(EntryEvent<String, Map<String, Long>> event) {
			// log.info("New versions table event received.");
			processAddOrUpdate(event);
		}

		@Override
		public void entryUpdated(EntryEvent<String, Map<String, Long>> event) {
			// log.info("Updated versions table event received.");
			processAddOrUpdate(event);
		}

		@Override
		public synchronized void entryRemoved(EntryEvent<String, Map<String, Long>> event) {
			check(event);
			// TODO: make this operation atomical. ConcurrentHashMap.clear() is not.
			log.info("Versions table removed!. Clearing up all tablespace versions.");
			context.getCurrentVersionsMap().clear();
			return;
		}

		@Override
		public void entryEvicted(EntryEvent<String, Map<String, Long>> event) {
			throw new RuntimeException("Should never happen. Something is really wrong :O");
		}
	}

	public void init(final SploutConfiguration config) throws Exception {
		this.config = config;
		log.info(this + " - Initializing QNode...");
		// Connect with the cluster.
		HazelcastInstance hz = Hazelcast.newHazelcastInstance(HazelcastConfigBuilder.build(config));
		int minutesToCheckRegister = config.getInt(HazelcastProperties.MAX_TIME_TO_CHECK_REGISTRATION, 5);
		int oldestMembersLeading = config.getInt(HazelcastProperties.OLDEST_MEMBERS_LEADING_COUNT, 3);
		// we must instantiate the DistributedRegistry even if we're not a DNode to be able to receive memembership leaving
		// in race conditions such as all DNodes leaving.
		new DistributedRegistry(CoordinationStructures.DNODES, null, hz, minutesToCheckRegister,
		    oldestMembersLeading);
		coord = new CoordinationStructures(hz);
		context = new QNodeHandlerContext(config, coord);
		// Initialialize DNodes tracking
		initDNodesTracking();
		// Initialize versions to be served tracking
		initVersionTracking();
		// Now instantiate modules
		deployer = new Deployer(context);
		querier = new Querier(context);
		// Get updated tablespace + version information
		context.synchronizeTablespaceVersions();
		log.info(Thread.currentThread() + " - Initializing QNode [DONE].");
		warmingThread = new Thread() {
			@Override
			public void run() {
				try {
					log.info("Currently warming up for [" + config.getInt(QNodeProperties.WARMING_TIME)
					    + "] - certain actions will only be taken afterwards.");
					Thread.sleep(config.getInt(QNodeProperties.WARMING_TIME) * 1000);
					log.info("Warming time ended [OK] Now the QNode will operate fully normally.");
				} catch(InterruptedException e) {
					log.error("Warming time interrupted - ");
				}
				isWarming.set(false);
			}
		};
		warmingThread.start();
	}

	/**
	 * Initializes the tracking of DNodes joining and leaving the cluster.
	 */
	private void initDNodesTracking() {
		IMap<String, DNodeInfo> dnodes = context.getCoordinationStructures().getDNodes();
		// CAUTION: We must register the listener BEFORE reading the list
		// of dnodes. Otherwise we could have a race condition.
		dnodes.addEntryListener(new DNodesListener(), true);
		Set<String> dNodes = new HashSet<String>();
		for(DNodeInfo dnodeInfo : dnodes.values()) {
			dNodes.add(dnodeInfo.getAddress());
			try {
				context.initializeThriftClientCacheFor(dnodeInfo.getAddress());
				context.updateTablespaceVersions(dnodeInfo, DNodeEvent.ENTRY);
			} catch(TablespaceVersionInfoException e) {
				throw new RuntimeException(e);
			} catch(TTransportException e) {
				throw new RuntimeException(e);
			} catch(InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
		log.info("Alive DNodes at QNode startup [" + Joiner.on(", ").skipNulls().join(dNodes) + "]");
		log.info("TablespaceVersion map at QNode startup [" + context.getTablespaceVersionsMap() + "]");
	}

	/**
	 * Loads the tablespaces information in memory to being ready to serve them, and starts to keep track of changes in
	 * tablespace's version to be served. To be called at initialization.
	 */
	private void initVersionTracking() throws IOException {
		IMap<String, Map<String, Long>> versions = context.getCoordinationStructures()
		    .getVersionsBeingServed();
		// CAUTION: We register the listener before updating the in memory versions
		// because if we do the other way around, we could lose updates to tablespace
		// versions or new tablespaces.
		VersionListener listener = new VersionListener();
		versions.addEntryListener(listener, true);
		String persistenceFolder = config.getString(HazelcastProperties.HZ_PERSISTENCE_FOLDER);
		if(persistenceFolder != null && !persistenceFolder.equals("")) {
			TablespaceVersionStore vStore = new TablespaceVersionStore(persistenceFolder);
			Map<String, Long> vBeingServedFromDisk = vStore
			    .load(CoordinationStructures.KEY_FOR_VERSIONS_BEING_SERVED);
			if(vBeingServedFromDisk != null) {
				Map<String, Long> vBeingServed = null;
				do {
					vBeingServed = context.getCoordinationStructures().getCopyVersionsBeingServed();
					if(vBeingServed != null) {
						// We assume info in memory (Hazelcast) is fresher than info in disk
						for(Map.Entry<String, Long> entry : vBeingServed.entrySet()) {
							vBeingServedFromDisk.put(entry.getKey(), entry.getValue());
						}
					}
				} while(!context.getCoordinationStructures().updateVersionsBeingServed(vBeingServed,
				    vBeingServedFromDisk));
				log.info("Loading tablespace versions to be served: " + vBeingServedFromDisk);
				updateLocalTablespace(vBeingServedFromDisk);
			}
		}
	}

	private void updateLocalTablespace(Map<String, Long> tablespacesAndVersions) throws IOException {
		log.info("Update local tablespace: " + tablespacesAndVersions);
		if(tablespacesAndVersions == null) {
			return;
		}
		// CAREFUL TODO: That is not atomic. Something should
		// be done to make that update atomic.
		context.getCurrentVersionsMap().putAll(tablespacesAndVersions);
		String persistenceFolder = config.getString(HazelcastProperties.HZ_PERSISTENCE_FOLDER);
		if(persistenceFolder != null && !persistenceFolder.equals("")) {
			TablespaceVersionStore vStore = new TablespaceVersionStore(persistenceFolder);
			vStore.store(CoordinationStructures.KEY_FOR_VERSIONS_BEING_SERVED, tablespacesAndVersions);
		}
	}

	/**
	 * Given a key, a tablespace and a SQL, query it to the appropriated DNode and return the result.
	 * <p>
	 * Returns a {@link QueryStatus}.
	 * 
	 * @throws QuerierException
	 */
	public QueryStatus query(String tablespace, String key, String sql, String partition)
	    throws JSONSerDeException, QuerierException {
		if(sql == null) {
			return new ErrorQueryStatus("Null sql provided, can't query.");
		}
		if(sql.length() < 1) {
			return new ErrorQueryStatus("Empty sql provided, can't query.");
		}
		if(key == null && partition == null) {
			return new ErrorQueryStatus(
			    "Null key / partition provided, can't query. Either partition or key must not be null.");
		}
		if(key != null && partition != null) {
			return new ErrorQueryStatus(
			    "(partition, key) parameters are mutually exclusive. Please use one or other, not both at the same time.");
		}
		meterQueriesServed.inc();
		meterRequestsPerSecond.mark();
		/*
		 * The queries are handled by the specialized module {@link Querier}
		 */
		QueryStatus result = querier.query(tablespace, key, sql, partition);
		if(result.getResult() != null) {
			meterResultSize.update(result.getResult().size());
		}
		return result;
	}

	/**
	 * Multi-query: use {@link Querier} for as many shards as needed and return a list of {@link QueryStatus}
	 * <p>
	 * Returns a list of {@link QueryStatus}.
	 */
	public ArrayList<QueryStatus> multiQuery(String tablespaceName, List<String> keyMins,
	    List<String> keyMaxs, String sql) throws JSONSerDeException {

		if(sql == null) {
			return new ArrayList<QueryStatus>(Arrays.asList(new QueryStatus[] { new ErrorQueryStatus(
			    "Null sql provided, can't query.") }));
		}
		if(sql.length() < 1) {
			return new ArrayList<QueryStatus>(Arrays.asList(new QueryStatus[] { new ErrorQueryStatus(
			    "Empty sql provided, can't query.") }));
		}

		if(keyMins.size() != keyMaxs.size()) {
			// This has to be handled before! We are not going to be polite here
			throw new RuntimeException(
			    "This is very likely a software bug: Inconsistent parameters received in "
			        + QNodeHandler.class + " for multiQuery() : " + tablespaceName + ", " + keyMins + ","
			        + keyMaxs + ", " + sql);
		}
		Set<Integer> impactedKeys = new HashSet<Integer>();
		Long version = context.getCurrentVersionsMap().get(tablespaceName);
		if(version == null) {
			return new ArrayList<QueryStatus>(Arrays.asList(new QueryStatus[] { new ErrorQueryStatus(
			    "No available version for tablespace " + tablespaceName) }));
		}
		// TODO Object creation (new TablespaceVersion), not very efficient for performance
		Tablespace tablespace = context.getTablespaceVersionsMap().get(
		    new TablespaceVersion(tablespaceName, version));
		if(tablespace == null) { // This can happen if, at startup, we only received the version and not the DNodeInfo
			return new ArrayList<QueryStatus>(Arrays.asList(new QueryStatus[] { new ErrorQueryStatus(
			    "No available information for tablespace version " + tablespaceName + "," + version) }));
		}
		if(keyMins.size() == 0) {
			impactedKeys.addAll(tablespace.getPartitionMap().findPartitions(null, null)); // all partitions are hit
		}
		for(int i = 0; i < keyMins.size(); i++) {
			impactedKeys.addAll(tablespace.getPartitionMap().findPartitions(keyMins.get(i), keyMaxs.get(i)));
		}
		ArrayList<QueryStatus> toReturn = new ArrayList<QueryStatus>();
		for(Integer shardKey : impactedKeys) {
			toReturn.add(querier.query(tablespaceName, sql, shardKey));
		}
		meterQueriesServed.inc();
		meterRequestsPerSecond.mark();
		return toReturn;
	}

	/**
	 * Given a list of {@link DeployRequest}, perform an asynchronous deploy. This is currently the most important part of
	 * Splout and the most complex one. Here we are involving several DNodes asynchronously and later we will check that
	 * everything finished.
	 * <p>
	 * Returns a {@link DeployInfo}.
	 */
	public DeployInfo deploy(List<DeployRequest> deployRequest) throws Exception {
		/*
		 * The deployment is handled by the specialized module {@link Deployer}
		 */
		return deployer.deploy(deployRequest);
	}

	/**
	 * Rollback: Set the version of some tablespaces to a particular one.
	 * <p>
	 * Returns a {@link StatusMessage}.
	 */
	public StatusMessage rollback(List<SwitchVersionRequest> rollbackRequest) throws JSONSerDeException {
		try {
			// TODO: Coordinate with context.synchronizeTablespaceVersions() because one could being deleting some tablespace
			// when other is trying a rollback.
			deployer.switchVersions(rollbackRequest);
			// TODO: Change this status message to something more programmatic
			return new StatusMessage("Done");

		} catch(UnexistingVersion e) {
			return new StatusMessage(e.getMessage() + ". Not possible to rollback to unexisting version.");
		}
	}

	/**
	 * Returns the {@link QNodeStatus} filled correctly.
	 */
	public QNodeStatus overview() throws Exception {
		QNodeStatus status = new QNodeStatus();
		status.setClusterSize(coord.getHz().getCluster().getMembers().size());
		Map<String, DNodeSystemStatus> aliveDNodes = new HashMap<String, DNodeSystemStatus>();
		for(DNodeInfo dnode : context.getCoordinationStructures().getDNodes().values()) {
			DNodeService.Client client = null;
			boolean renew = false;
			try {
				client = getContext().getDNodeClientFromPool(dnode.getAddress());
				aliveDNodes.put(dnode.getAddress(), JSONSerDe.deSer(client.status(), DNodeSystemStatus.class));
			} catch(TTransportException e) {
				renew = true;
				throw e;
			} finally {
				if(client != null) {
					context.returnDNodeClientToPool(dnode.getAddress(), client, renew);
				}
			}
		}
		status.setdNodes(aliveDNodes);
		Map<String, Tablespace> tablespaceMap = new HashMap<String, Tablespace>();
		for(Map.Entry<String, Long> currentVersion : context.getCurrentVersionsMap().entrySet()) {
			Tablespace tablespace = context.getTablespaceVersionsMap().get(
			    new TablespaceVersion(currentVersion.getKey(), currentVersion.getValue()));
			if(tablespace != null) { // this might happen and it is not a bug
				tablespaceMap.put(currentVersion.getKey(), tablespace);
			}
		}
		status.setTablespaceMap(tablespaceMap);
		return status;
	}

	/**
	 * Returns the list of tablespaces
	 */
	public Set<String> tablespaces() throws Exception {
		return context.getCurrentVersionsMap().keySet();
	}

	/**
	 * Return all available versions for each tablespace
	 */
	@Override
	public Map<Long, Tablespace> allTablespaceVersions(final String tablespace) throws Exception {
		HashMap<Long, Tablespace> ret = new HashMap<Long, Tablespace>();
		Set<Entry<TablespaceVersion, Tablespace>> versions = context.getTablespaceVersionsMap().entrySet();
		for(Entry<TablespaceVersion, Tablespace> entry : versions) {
			if(entry.getKey().getTablespace().equals(tablespace)) {
				ret.put(entry.getKey().getVersion(), entry.getValue());
			}
		}
		return ret;
	}

	/**
	 * Return a properly filled {@link DNodeSystemStatus}
	 */
	@Override
	public DNodeSystemStatus dnodeStatus(String dnode) throws Exception {
		DNodeService.Client client = null;
		boolean renew = false;
		try {
			client = getContext().getDNodeClientFromPool(dnode);
			return JSONSerDe.deSer(client.status(), DNodeSystemStatus.class);
		} catch(TTransportException e) {
			renew = true;
			throw e;
		} finally {
			if(client != null) {
				context.returnDNodeClientToPool(dnode, client, renew);
			}
		}
	}

	@Override
	public Tablespace tablespace(String tablespace) throws Exception {
		Long version = context.getCurrentVersionsMap().get(tablespace);
		if(version == null) {
			return null;
		}
		Tablespace t = context.getTablespaceVersionsMap().get(new TablespaceVersion(tablespace, version));
		return t;
	}

	/**
	 * Get the list of DNodes
	 */
	@Override
	public List<String> getDNodeList() throws Exception {
		return context.getDNodeList();
	}

	/**
	 * Properly dispose this QNodeHandler.
	 */
	@Override
	public void close() throws Exception {
		context.close();
		if(warmingThread != null) {
			warmingThread.interrupt();
			warmingThread.join();
		}
	}

	/**
	 * Used for testing.
	 */
	public QNodeHandlerContext getContext() {
		return context;
	}

	/**
	 * Used for testing.
	 */
	public Deployer getDeployer() {
		return deployer;
	}
}