package com.splout.db.qnode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.splout.db.common.PartitionEntry;
import com.splout.db.common.PartitionMap;
import com.splout.db.common.ReplicationEntry;
import com.splout.db.common.ReplicationMap;
import com.splout.db.common.Tablespace;
import com.splout.db.hazelcast.DNodeInfo;
import com.splout.db.hazelcast.TablespaceVersion;
import com.splout.db.qnode.QNodeHandlerContext.DNodeEvent;
import com.splout.db.qnode.QNodeHandlerContext.TablespaceVersionInfoException;
import com.splout.db.thrift.PartitionMetadata;

/**
 * Abstraction that allows any Hazelcast listener to build an incremental state of the tablespaces
 * and the versions everytime that a new DNode enters or leaves.
 */
public class TablespaceMemoryState {

  protected final static Log log = LogFactory.getLog(TablespaceMemoryState.class);

  // Local map with all versions for a tablespace with the PartitionMap,
  // ReplicationMap for each of them
  private final Map<TablespaceVersion, Tablespace> tablespaceVersionsMap = new ConcurrentHashMap<TablespaceVersion, Tablespace>();

  public Object tVLock = new Object();

  /**
   * Update the in-memory <TablespaceVersion, Tablespace> map when a DNode
   * joins, leaves or updates its DNodeINfo.
   */
  public synchronized void updateTablespaceVersions(DNodeInfo dNodeInfo, DNodeEvent event)
      throws TablespaceVersionInfoException {
    Map<TablespaceVersion, Tablespace> tablespaceVersionMap = getTablespaceVersionsMap();

    // First check if this DNode is not anymore serving a version that it used
    // to serve (IMPLICIT leaving).
    // This can happen for instance if a DNode removes an old version.
    // In this case the version will eventually become empty here.
    Iterator<Map.Entry<TablespaceVersion, Tablespace>> iterator = tablespaceVersionMap.entrySet().iterator();

    while (iterator.hasNext()) {
      Map.Entry<TablespaceVersion, Tablespace> tablespaceVersion = iterator.next();
      String tablespaceName = tablespaceVersion.getKey().getTablespace();
      Long version = tablespaceVersion.getKey().getVersion();
      // Is this DNode present in this version?
      Tablespace tablespace = tablespaceVersion.getValue();
      // We will rebuild the replication map to check if it became empty after
      // the checkings or not
      int nonEmptyReplicas = 0;

      Iterator<ReplicationEntry> repIter = tablespace.getReplicationMap().getReplicationEntries().iterator();
      while (repIter.hasNext()) {
        ReplicationEntry entry = repIter.next();
        int partition = entry.getShard();
        if (entry.getNodes().contains(dNodeInfo.getAddress())) {
          // Yes!
          // So we have to check if this DNode is still serving this
          // version/partition or not
          if ((dNodeInfo.getServingInfo().get(tablespaceName) == null)
              || (dNodeInfo.getServingInfo().get(tablespaceName).get(version) == null)
              || (dNodeInfo.getServingInfo().get(tablespaceName).get(version).get(partition) == null)) {
            // NO! So we have to remove the DNode
            entry.getNodes().remove(dNodeInfo.getAddress());
            if (entry.getNodes().isEmpty()) {
              repIter.remove();
              // Remove also from PartitionMap
              PartitionEntry pEntry = new PartitionEntry();
              pEntry.setShard(entry.getShard());
              tablespace.getPartitionMap().getPartitionEntries().remove(pEntry);
            }
          }
        }
        if (!entry.getNodes().isEmpty()) {
          nonEmptyReplicas++;
        }
      }
      if (nonEmptyReplicas == 0) {
        // Delete TablespaceVersion
        log.info("Removing empty tablespace version (implicit leaving from " + dNodeInfo.getAddress() + "): "
            + tablespaceName + ", " + version);
        iterator.remove();
      }
    }

    // Now iterate over all the tablespaces of this DNode to see new additions
    // or EXPLICIT leavings
    for (Map.Entry<String, Map<Long, Map<Integer, PartitionMetadata>>> tablespaceEntry : dNodeInfo.getServingInfo()
        .entrySet()) {
      String tablespaceName = tablespaceEntry.getKey();
      // Iterate over all versions of this tablespace
      for (Map.Entry<Long, Map<Integer, PartitionMetadata>> versionEntry : tablespaceEntry.getValue().entrySet()) {
        Long versionName = versionEntry.getKey();
        TablespaceVersion tablespaceVersion = new TablespaceVersion(tablespaceName, versionName);
        Tablespace currentTablespace = tablespaceVersionMap.get(tablespaceVersion);
        List<PartitionEntry> partitionMap = new ArrayList<PartitionEntry>();
        List<ReplicationEntry> replicationMap = new ArrayList<ReplicationEntry>();
        long deployDate = -1;
        if (currentTablespace != null) {
          // Not first time we see this tablespace. We do a copy of the
          // partition map to be able to modify it without
          // contention.
          partitionMap.addAll(currentTablespace.getPartitionMap().getPartitionEntries());
          replicationMap.addAll(currentTablespace.getReplicationMap().getReplicationEntries());
          deployDate = currentTablespace.getCreationDate();
        }
        // Iterate over all partitions of this tablespace
        for (Map.Entry<Integer, PartitionMetadata> partition : versionEntry.getValue().entrySet()) {
          deployDate = deployDate == -1 ? partition.getValue().getDeploymentDate() : deployDate;
          if (deployDate != -1 && (deployDate != partition.getValue().getDeploymentDate())) {
            throw new TablespaceVersionInfoException(
                "Inconsistent partition metadata within same node, deploy date was " + deployDate + " versus "
                    + partition.getValue().getDeploymentDate());
          }
          PartitionMetadata metadata = partition.getValue();
          Integer shard = partition.getKey();
          // Create a PartitionEntry according to this PartitionMetadata
          PartitionEntry myEntry = new PartitionEntry();
          myEntry.setMax(metadata.getMaxKey());
          myEntry.setMin(metadata.getMinKey());
          myEntry.setShard(shard);
          PartitionEntry existingPartitionEntry = null;
          // Look for an existing PartitionEntry for the same shard in the
          // PartitionMap
          if (!partitionMap.contains(myEntry)) {
            if (!event.equals(DNodeEvent.LEAVE)) {
              // In this case all conditions are met for adding a new entry to
              // the PartitionMap
              partitionMap.add(myEntry);
              // Note that now the PartitionMap is not necessarily sorted! let's
              // sort it now
              Collections.sort(partitionMap);
            }
          } else {
            // Check consistency of this Partition Metadata
            existingPartitionEntry = partitionMap.get(partitionMap.indexOf(myEntry));
            if (existingPartitionEntry.getMax() == null || myEntry.getMax() == null) {
              if (!(existingPartitionEntry.getMax() == null && myEntry.getMax() == null)) {
                throw new TablespaceVersionInfoException("Inconsistent partition metadata between nodes: "
                    + existingPartitionEntry + " versus " + myEntry);
              }
            } else {
              if (!existingPartitionEntry.getMax().equals(myEntry.getMax())) {
                throw new TablespaceVersionInfoException("Inconsistent partition metadata between nodes: "
                    + existingPartitionEntry + " versus " + myEntry);
              }
            }
            if (existingPartitionEntry.getMin() == null || myEntry.getMin() == null) {
              if (!(existingPartitionEntry.getMin() == null && myEntry.getMin() == null)) {
                throw new TablespaceVersionInfoException("Inconsistent partition metadata between nodes: "
                    + existingPartitionEntry + " versus " + myEntry);
              }
            } else {
              if (!existingPartitionEntry.getMin().equals(myEntry.getMin())) {
                throw new TablespaceVersionInfoException("Inconsistent partition metadata between nodes: "
                    + existingPartitionEntry + " versus " + myEntry);
              }
            }
          }
          // Create a ReplicationEntry according to this PartitionMetadata
          // Will only contain this DNode as we don't know about the others yet
          ReplicationEntry reEntry = new ReplicationEntry();
          reEntry.setShard(shard);
          reEntry.setExpectedReplicationFactor(metadata.getNReplicas());
          reEntry.setNodes(new ArrayList<String>());
          // Look for an existing ReplicationEntry for the same shard in the
          // ReplicationMap
          if (replicationMap.contains(reEntry)) {
            ReplicationEntry existingEntry = replicationMap.get(replicationMap.indexOf(reEntry));
            if (event.equals(DNodeEvent.LEAVE)) {
              // Remove it from replication map and partition map
              existingEntry.getNodes().remove(dNodeInfo.getAddress());
              if (existingEntry.getNodes().isEmpty()) {
                replicationMap.remove(existingEntry);
                if (existingPartitionEntry != null) {
                  partitionMap.remove(existingPartitionEntry);
                } else {
                  throw new RuntimeException(
                      "ReplicationEntry for one shard with no associated PartitionEntry. This is very likely to be a software bug.");
                }
              }
            } else {
              if (!existingEntry.getNodes().contains(dNodeInfo.getAddress())) {
                // Add it to replication map
                existingEntry.getNodes().add(dNodeInfo.getAddress());
              } else {
                // We are adding / updating but the node already exists in the
                // replication map.
              }
            }
          } else if (!event.equals(DNodeEvent.LEAVE)) { // Otherwise just add
                                                        // and sort
            // We check the DNodeEvent but although would be very weird if this
            // DNode leaves and its ReplicationEntry
            // wasn't present
            reEntry.getNodes().add(dNodeInfo.getAddress());
            replicationMap.add(reEntry);
            Collections.sort(reEntry.getNodes());
            Collections.sort(replicationMap);
          }
        }
        // Delete tablespaceVersion if it is empty now
        if (currentTablespace != null && replicationMap.size() == 0) {
          log.info("Removing empty tablespaceVersion: " + tablespaceVersion + " due to explicit leaving from node "
              + dNodeInfo.getAddress());
          tablespaceVersionMap.remove(tablespaceVersion);
        } else {
          // Update the info in memory
          currentTablespace = new Tablespace(new PartitionMap(partitionMap), new ReplicationMap(replicationMap),
              versionName, deployDate);
          tablespaceVersionMap.put(tablespaceVersion, currentTablespace);
        }
      }
    }
  }

  public Map<TablespaceVersion, Tablespace> getTablespaceVersionsMap() {
    return tablespaceVersionsMap;
  }
}
