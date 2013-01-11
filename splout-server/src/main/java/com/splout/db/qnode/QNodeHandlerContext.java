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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;
import com.splout.db.common.PartitionEntry;
import com.splout.db.common.PartitionMap;
import com.splout.db.common.ReplicationEntry;
import com.splout.db.common.ReplicationMap;
import com.splout.db.common.SploutConfiguration;
import com.splout.db.common.Tablespace;
import com.splout.db.dnode.DNodeClient;
import com.splout.db.hazelcast.CoordinationStructures;
import com.splout.db.hazelcast.DNodeInfo;
import com.splout.db.hazelcast.TablespaceVersion;
import com.splout.db.thrift.DNodeException;
import com.splout.db.thrift.DNodeService;
import com.splout.db.thrift.PartitionMetadata;

/**
 * This class contains the basic context of {@link QNodeHandler}. This context involves in-memory information of the
 * system such as: list of alive DNodes, list of tablespaces, versions and so forth. In addition, this class also
 * maintains a pool of connections to the DNodes. This class is shared among all different {@link QNodeHandlerModule}
 * such as {@link Deployer} so that each specialized module can have access to the context.
 */
public class QNodeHandlerContext {

	protected final static Log log = LogFactory.getLog(QNodeHandlerContext.class);

	// This map indicates which is the current version being served. It has to be updated atomically.
	private final Map<String, Long> currentVersionsMap = new ConcurrentHashMap<String, Long>();
	// The SploutConfiguration
	private SploutConfiguration config;
	// The coordination structures that use Hazelcast underneath
	private CoordinationStructures coordinationStructures;
	// Local map with all versions for a tablespace with the PartitionMap, ReplicationMap for each of them
	private final Map<TablespaceVersion, Tablespace> tablespaceVersionsMap = new ConcurrentHashMap<TablespaceVersion, Tablespace>();

	// The per-DNode Thrift client pools
	private ConcurrentMap<String, BlockingQueue<DNodeService.Client>> thriftClientCache = new ConcurrentHashMap<String, BlockingQueue<DNodeService.Client>>();
	private ReentrantLock thriftClientCacheLock = new ReentrantLock();

	private int thriftClientPoolSize;

	public QNodeHandlerContext(SploutConfiguration config, CoordinationStructures coordinationStructures) {
		this.config = config;
		this.coordinationStructures = coordinationStructures;
		this.thriftClientPoolSize = config.getInt(QNodeProperties.DNODE_POOL_SIZE);
	}

	public static enum DNodeEvent {
		LEAVE, ENTRY, UPDATE
	}

	@SuppressWarnings("serial")
	public final static class TablespaceVersionInfoException extends Exception {

		public TablespaceVersionInfoException(String msg) {
			super(msg);
		}
	}

	public Object tVLock = new Object();

	/**
	 * Update the in-memory <TablespaceVersion, Tablespace> map when a DNode joins, leaves or updates its DNodeINfo.
	 */
	public void updateTablespaceVersions(DNodeInfo dNodeInfo, DNodeEvent event)
	    throws TablespaceVersionInfoException {
		synchronized(tVLock) {
			Map<TablespaceVersion, Tablespace> tablespaceVersionMap = getTablespaceVersionsMap();

			// First check if this DNode is not anymore serving a version that it used to serve (IMPLICIT leaving).
			// This can happen for instance if a DNode removes an old version.
			// In this case the version will eventually become empty here.
			Iterator<Map.Entry<TablespaceVersion, Tablespace>> iterator = tablespaceVersionMap.entrySet()
			    .iterator();

			while(iterator.hasNext()) {
				Map.Entry<TablespaceVersion, Tablespace> tablespaceVersion = iterator.next();
				String tablespaceName = tablespaceVersion.getKey().getTablespace();
				Long version = tablespaceVersion.getKey().getVersion();
				// Is this DNode present in this version?
				Tablespace tablespace = tablespaceVersion.getValue();
				// We will rebuild the replication map to check if it became empty after the checkings or not
				int nonEmptyReplicas = 0;

				Iterator<ReplicationEntry> repIter = tablespace.getReplicationMap().getReplicationEntries()
				    .iterator();
				while(repIter.hasNext()) {
					ReplicationEntry entry = repIter.next();
					int partition = entry.getShard();
					if(entry.getNodes().contains(dNodeInfo.getAddress())) {
						// Yes!
						// So we have to check if this DNode is still serving this version/partition or not
						if((dNodeInfo.getServingInfo().get(tablespaceName) == null)
						    || (dNodeInfo.getServingInfo().get(tablespaceName).get(version) == null)
						    || (dNodeInfo.getServingInfo().get(tablespaceName).get(version).get(partition) == null)) {
							// NO! So we have to remove the DNode
							entry.getNodes().remove(dNodeInfo.getAddress());
							if(entry.getNodes().isEmpty()) {
								repIter.remove();
								// Remove also from PartitionMap
								PartitionEntry pEntry = new PartitionEntry();
								pEntry.setShard(entry.getShard());
								tablespace.getPartitionMap().getPartitionEntries().remove(pEntry);
							}
						}
					}
					if(!entry.getNodes().isEmpty()) {
						nonEmptyReplicas++;
					}
				}
				if(nonEmptyReplicas == 0) {
					// Delete TablespaceVersion
					log.info("Removing empty tablespace version (implicit leaving from " + dNodeInfo.getAddress()
					    + "): " + tablespaceName + ", " + version);
					iterator.remove();
				}
			}

			// Now iterate over all the tablespaces of this DNode to see new additions or EXPLICIT leavings
			for(Map.Entry<String, Map<Long, Map<Integer, PartitionMetadata>>> tablespaceEntry : dNodeInfo
			    .getServingInfo().entrySet()) {
				String tablespaceName = tablespaceEntry.getKey();
				// Iterate over all versions of this tablespace
				for(Map.Entry<Long, Map<Integer, PartitionMetadata>> versionEntry : tablespaceEntry.getValue()
				    .entrySet()) {
					Long versionName = versionEntry.getKey();
					TablespaceVersion tablespaceVersion = new TablespaceVersion(tablespaceName, versionName);
					Tablespace currentTablespace = tablespaceVersionMap.get(tablespaceVersion);
					List<PartitionEntry> partitionMap = new ArrayList<PartitionEntry>();
					List<ReplicationEntry> replicationMap = new ArrayList<ReplicationEntry>();
					long deployDate = -1;
					if(currentTablespace != null) {
						// Not first time we see this tablespace. We do a copy of the partition map to be able to modify it without
						// contention.
						partitionMap.addAll(currentTablespace.getPartitionMap().getPartitionEntries());
						replicationMap.addAll(currentTablespace.getReplicationMap().getReplicationEntries());
						deployDate = currentTablespace.getCreationDate();
					}
					// Iterate over all partitions of this tablespace
					for(Map.Entry<Integer, PartitionMetadata> partition : versionEntry.getValue().entrySet()) {
						deployDate = deployDate == -1 ? partition.getValue().getDeploymentDate() : deployDate;
						if(deployDate != -1 && (deployDate != partition.getValue().getDeploymentDate())) {
							throw new TablespaceVersionInfoException(
							    "Inconsistent partition metadata within same node, deploy date was " + deployDate
							        + " versus " + partition.getValue().getDeploymentDate());
						}
						PartitionMetadata metadata = partition.getValue();
						Integer shard = partition.getKey();
						// Create a PartitionEntry according to this PartitionMetadata
						PartitionEntry myEntry = new PartitionEntry();
						myEntry.setMax(metadata.getMaxKey());
						myEntry.setMin(metadata.getMinKey());
						myEntry.setShard(shard);
						PartitionEntry existingPartitionEntry = null;
						// Look for an existing PartitionEntry for the same shard in the PartitionMap
						if(!partitionMap.contains(myEntry)) {
							if(!event.equals(DNodeEvent.LEAVE)) {
								// In this case all conditions are met for adding a new entry to the PartitionMap
								partitionMap.add(myEntry);
								// Note that now the PartitionMap is not necessarily sorted! let's sort it now
								Collections.sort(partitionMap);
							}
						} else {
							// Check consistency of this Partition Metadata
							existingPartitionEntry = partitionMap.get(partitionMap.indexOf(myEntry));
							if(existingPartitionEntry.getMax() == null || myEntry.getMax() == null) {
								if(!(existingPartitionEntry.getMax() == null && myEntry.getMax() == null)) {
									throw new TablespaceVersionInfoException(
									    "Inconsistent partition metadata between nodes: " + existingPartitionEntry
									        + " versus " + myEntry);
								}
							} else {
								if(!existingPartitionEntry.getMax().equals(myEntry.getMax())) {
									throw new TablespaceVersionInfoException(
									    "Inconsistent partition metadata between nodes: " + existingPartitionEntry
									        + " versus " + myEntry);
								}
							}
							if(existingPartitionEntry.getMin() == null || myEntry.getMin() == null) {
								if(!(existingPartitionEntry.getMin() == null && myEntry.getMin() == null)) {
									throw new TablespaceVersionInfoException(
									    "Inconsistent partition metadata between nodes: " + existingPartitionEntry
									        + " versus " + myEntry);
								}
							} else {
								if(!existingPartitionEntry.getMin().equals(myEntry.getMin())) {
									throw new TablespaceVersionInfoException(
									    "Inconsistent partition metadata between nodes: " + existingPartitionEntry
									        + " versus " + myEntry);
								}
							}
						}
						// Create a ReplicationEntry according to this PartitionMetadata
						// Will only contain this DNode as we don't know about the others yet
						ReplicationEntry reEntry = new ReplicationEntry();
						reEntry.setShard(shard);
						reEntry.setNodes(new ArrayList<String>());
						// Look for an existing ReplicationEntry for the same shard in the ReplicationMap
						if(replicationMap.contains(reEntry)) {
							ReplicationEntry existingEntry = replicationMap.get(replicationMap.indexOf(reEntry));
							if(event.equals(DNodeEvent.LEAVE)) {
								// Remove it from replication map and partition map
								existingEntry.getNodes().remove(dNodeInfo.getAddress());
								if(existingEntry.getNodes().isEmpty()) {
									replicationMap.remove(existingEntry);
									if(existingPartitionEntry != null) {
										partitionMap.remove(existingPartitionEntry);
									} else {
										throw new RuntimeException(
										    "ReplicationEntry for one shard with no associated PartitionEntry. This is very likely to be a software bug.");
									}
								}
							} else {
								if(!existingEntry.getNodes().contains(dNodeInfo.getAddress())) {
									// Add it to replication map
									existingEntry.getNodes().add(dNodeInfo.getAddress());
								} else {
									// We are adding / updating but the node already exists in the replication map. Check consistency here
									// TODO We are not saving the expected replication factor anywhere currently
								}
							}
						} else if(!event.equals(DNodeEvent.LEAVE)) { // Otherwise just add and sort
							// We check the DNodeEvent but although would be very weird if this DNode leaves and its ReplicationEntry
							// wasn't present
							reEntry.getNodes().add(dNodeInfo.getAddress());
							replicationMap.add(reEntry);
							Collections.sort(replicationMap);
						}
					}
					// Delete tablespaceVersion if it is empty now
					if(currentTablespace != null && replicationMap.size() == 0) {
						log.info("Removing empty tablespaceVersion: " + tablespaceVersion
						    + " due to explicit leaving from node " + dNodeInfo.getAddress());
						tablespaceVersionMap.remove(tablespaceVersion);
					} else {
						// Update the info in memory
						currentTablespace = new Tablespace(new PartitionMap(partitionMap), new ReplicationMap(
						    replicationMap), versionName, deployDate);
						tablespaceVersionMap.put(tablespaceVersion, currentTablespace);
					}
				}
			}
		}
	}

	/**
	 * This method can be called to initialize a pool of connections to a dnode. This method may be called from multiple
	 * threads so it should be safe to call it concurrently.
	 */
	public void initializeThriftClientCacheFor(String dnode) throws TTransportException,
	    InterruptedException {
		// this lock is on the whole cache but we would actually be interested in a per-DNode lock...
		// there's only one lock for simplicity.
		thriftClientCacheLock.lock();
		try {
			// initialize queue for this DNode
			BlockingQueue<DNodeService.Client> dnodeQueue = thriftClientCache.get(dnode);
			if(dnodeQueue == null) {
				// this assures that the per-DNode queue is only created once and then reused.
				dnodeQueue = new LinkedBlockingDeque<DNodeService.Client>(thriftClientPoolSize);
			}
			if(dnodeQueue.isEmpty()) {
				try {
					for(int i = dnodeQueue.size(); i < thriftClientPoolSize; i++) {
						dnodeQueue.put(DNodeClient.get(dnode));
					}
					// we only put the queue if all connections have been populated
					thriftClientCache.put(dnode, dnodeQueue);
				} catch(TTransportException e) {
					log.error("Error while trying to populate queue for " + dnode
					    + ", will discard created connections.", e);
					while(!dnodeQueue.isEmpty()) {
						dnodeQueue.poll().getOutputProtocol().getTransport().close();
					}
					throw e;
				}
			} else {
				// it should be safe to call this method from different places concurrently
				// so we contemplate the case where another Thread already populated the queue
				// and only populate it if it's really empty.
				log.warn(Thread.currentThread().getName() + " : queue for [" + dnode
				    + "] is not empty - it was populated before.");
			}
		} finally {
			thriftClientCacheLock.unlock();
		}
	}

	/**
	 * This method can be called by {@link QNodeHandler} to cancel the Thrift client cache when a DNode disconnects.
	 * Usually this happens when Hazelcast notifies it.
	 */
	public void discardThriftClientCacheFor(String dnode) throws InterruptedException {
		thriftClientCacheLock.lock();
		try {
			// discarding all connections to a DNode who leaved
			log.info(Thread.currentThread().getName() + " : trashing queue for [" + dnode + "] as it leaved.");
			BlockingQueue<DNodeService.Client> dnodeQueue = thriftClientCache.get(dnode);
			// release connections until empty
			while(!dnodeQueue.isEmpty()) {
				dnodeQueue.take().getOutputProtocol().getTransport().close();
			}
			thriftClientCache.remove(dnode); // to indicate that the DNode is not present
		} finally {
			thriftClientCacheLock.unlock();
		}
	}

	/**
	 * Get the Thrift client for this DNode.
	 */
	public DNodeService.Client getDNodeClientFromPool(String dnode) throws TTransportException {
		try {
			BlockingQueue<DNodeService.Client> dnodeQueue;
			thriftClientCacheLock.lock();
			try {
				dnodeQueue = thriftClientCache.get(dnode);
				if(dnodeQueue == null) {
					// This shouldn't happen in real life because it is initialized by the QNode, but it is useful for unit
					// testing.
					initializeThriftClientCacheFor(dnode);
					dnodeQueue = thriftClientCache.get(dnode);
				}
			} finally {
				thriftClientCacheLock.unlock();
			}

			DNodeService.Client client = dnodeQueue.take();
			return client;
		} catch(InterruptedException e) {
			log.error("Interrupted", e);
			return null;
		}
	}

	/**
	 * Return a Thrift client to the pool. This method is a bit tricky since we may want to return a connection when a
	 * DNode already disconnected. Also, if the QNode is closing, we don't want to leave opened sockets around. To do it
	 * safely, we check in a loop whether 1) we are closing / cleaning the QNode or 2) the DNode has disconnected.
	 */
	public void returnDNodeClientToPool(String dnode, DNodeService.Client client, boolean renew) {
		if(renew) {
			// renew -> try to close if not properly close and set to null
			client.getOutputProtocol().getTransport().close();
			client = null;
		}
		do {
			if(closing.get()) { // don't return to the pool if the system is already closing! we must close everything!
				if(client != null) {
					client.getOutputProtocol().getTransport().close();
				}
				return;
			}
			BlockingQueue<DNodeService.Client> dnodeQueue = thriftClientCache.get(dnode);
			if(dnodeQueue == null) {
				// dnode is not connected, so we exit.
				if(client != null) {
					client.getOutputProtocol().getTransport().close();
				}
				return;
			}
			if(client == null) { // we have to renew the connection
				// endless loop, we need to get a new client otherwise the queue will not have the same size!
				try {
					client = DNodeClient.get(dnode);
				} catch(TTransportException e) {
					log.error(
					    "TTransportException while creating client to renew. Trying again, we can't exit here!", e);
				}
			} else { // we have a valid client so let's put it into the queue
				try {
					dnodeQueue.add(client);
				} catch(IllegalStateException e) {
					client.getOutputProtocol().getTransport().close();
					log.error("Trying to return a connection for dnode ["
					    + dnode
					    + "] but the pool already has the maximum number of connections. This is likely a software bug!.");
					throw new RuntimeException(
					    "Trying to return a connection for dnode ["
					        + dnode
					        + "] but the pool already has the maximum number of connections. This is likely a software bug!.");
				}
			}
		} while(client == null);
		if(closing.get()) { // one last check to avoid not closing every socket.
			if(client != null) {
				client.getOutputProtocol().getTransport().close();
			}
		}
	}

	/**
	 * Rotates the versions (deletes versions that are old or useless). To be executed at startup and after a deployment.
	 */
	public List<com.splout.db.thrift.TablespaceVersion> synchronizeTablespaceVersions()
	    throws InterruptedException {
		log.info("Starting to look for old tablespace versions to remove...");

		int maxVersionsPerTablespace = config.getInt(QNodeProperties.VERSIONS_PER_TABLESPACE);

		// Will contain the list of versions per each tablespace, sorted by creation date descendant
		TreeMultimap<String, Tablespace> tablespaces = TreeMultimap.create(Ordering.natural(),
		    new Comparator<Tablespace>() {
			    @Override
			    public int compare(Tablespace tb1, Tablespace tb2) {
				    // reverse ordering. Older dates appears LAST. If same date, then version is compared.
				    int comp = -((Long) tb1.getCreationDate()).compareTo(tb2.getCreationDate());
				    if(comp == 0) {
					    return -((Long) tb1.getVersion()).compareTo(tb2.getVersion());
				    } else {
					    return comp;
				    }
			    }
		    });

		Map<TablespaceVersion, Tablespace> myTablespaces = getTablespaceVersionsMap();

		// We build a in memory version of tablespaces for analyzing it
		// and prune old ones.
		for(Entry<TablespaceVersion, Tablespace> entry : myTablespaces.entrySet()) {
			tablespaces.put(entry.getKey().getTablespace(), entry.getValue());
		}

		// We will remove only versions older than the one being served
		Map<String, Long> hzVersionsBeingServed = coordinationStructures.getCopyVersionsBeingServed();
		if(hzVersionsBeingServed == null) {
			log.info("... No versions yet being served.");
			return null; // nothing to do yet
		}

		List<com.splout.db.thrift.TablespaceVersion> tablespacesToRemove = new ArrayList<com.splout.db.thrift.TablespaceVersion>();

		for(Entry<String, Long> entry : hzVersionsBeingServed.entrySet()) {
			String tablespace = entry.getKey();
			Long versionBeingServed = entry.getValue();
			// Tablespaces are sorted by creation date desc.
			SortedSet<Tablespace> allVersions = tablespaces.get(tablespace);
			Iterator<Tablespace> it = allVersions.iterator();
			boolean foundVersionBeingServed = false;
			int countVersionsAfter = 0;
			while(it.hasNext()) {
				Tablespace tb = it.next();
				if(versionBeingServed.equals(tb.getVersion())) {
					foundVersionBeingServed = true;
				} else {
					if(foundVersionBeingServed) {
						countVersionsAfter++;
						if(countVersionsAfter >= maxVersionsPerTablespace) {
							// This is the case where we remove the version
							// 1 - This tablespace has a version being served
							// 2 - This version is older than the current tablespace being served
							// 3 - We are already keeping maxVersionsPerTablespace versions
							tablespacesToRemove.add(new com.splout.db.thrift.TablespaceVersion(tablespace, tb
							    .getVersion()));
							log.info("Tablespace [" + tablespace + "] Version [" + tb.getVersion() + "] "
							    + "created at [" + new Date(tb.getCreationDate())
							    + "] REMOVED. We already keep younger versions.");
						}
					}
				}
			}

			if(!foundVersionBeingServed) {
				log.info("Tablespace [" + tablespace
				    + "] without any version being served. Please, have a look, and remove them if not used");
			}

			if(tablespacesToRemove.size() > 0) {
				log.info("Sending [" + tablespacesToRemove + "] to all alive DNodes.");
				for(DNodeInfo dnode : coordinationStructures.getDNodes().values()) {
					DNodeService.Client client = null;
					boolean renew = false;
					try {
						client = getDNodeClientFromPool(dnode.getAddress());
						client.deleteOldVersions(tablespacesToRemove);
					} catch(TTransportException e) {
						renew = true;
						log.warn("Failed sending delete TablespaceVersions order to (" + dnode
						    + "). Not critical as they will be removed after other deployments.", e);
					} catch(DNodeException e) {
						log.warn("Failed sending delete TablespaceVersions order to (" + dnode
						    + "). Not critical as they will be removed after other deployments.", e);
					} catch(TException e) {
						log.warn("Failed sending delete TablespaceVersions order to (" + dnode
						    + "). Not critical as they will be removed after other deployments.", e);
					} finally {
						if(client != null) {
							returnDNodeClientToPool(dnode.getAddress(), client, renew);
						}
					}
				}
			}
			log.info("... done looking for old tablespace versions to remove...");
		}

		return tablespacesToRemove; // Return for unit test
	}

	private AtomicBoolean closing = new AtomicBoolean(false);

	public void close() {
		closing.set(true); // will indicate other parts of this code that things have to be closed!
		for(Map.Entry<String, BlockingQueue<DNodeService.Client>> entry : thriftClientCache.entrySet()) {
			while(entry.getValue().size() > 0) {
				try {
					entry.getValue().take().getOutputProtocol().getTransport().close();
				} catch(InterruptedException e) {
					log.error("Interrupted!", e);
				}
			}
		}
	}

	// ---- Getters ---- //

	public Map<String, Long> getCurrentVersionsMap() {
		return currentVersionsMap;
	}

	public Map<TablespaceVersion, Tablespace> getTablespaceVersionsMap() {
		return tablespaceVersionsMap;
	}

	public CoordinationStructures getCoordinationStructures() {
		return coordinationStructures;
	}

	public SploutConfiguration getConfig() {
		return config;
	}

	public ConcurrentMap<String, BlockingQueue<DNodeService.Client>> getThriftClientCache() {
		return thriftClientCache;
	}
}