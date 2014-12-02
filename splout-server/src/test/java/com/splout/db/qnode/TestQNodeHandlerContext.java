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

import com.splout.db.common.ReplicationEntry;
import com.splout.db.common.SploutConfiguration;
import com.splout.db.common.Tablespace;
import com.splout.db.hazelcast.DNodeInfo;
import com.splout.db.hazelcast.TablespaceVersion;
import com.splout.db.qnode.QNodeHandlerContext.DNodeEvent;
import com.splout.db.qnode.QNodeHandlerContext.TablespaceVersionInfoException;
import com.splout.db.thrift.PartitionMetadata;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Unit tests for the business logic inside {@link QNodeHandlerContext}
 */
public class TestQNodeHandlerContext {

  private final static PartitionMetadata DEFAULT_METADATA = new PartitionMetadata();

  public static class DNodeInfoFacade {

    String address;
    String httpAddress;
    Map<String, Map<Long, Map<Integer, PartitionMetadata>>> servingInfo;

    public DNodeInfoFacade(String address) {
      this(address, address);
    }

    public DNodeInfoFacade(String address, String httpAddress) {
      this.address = address;
      this.httpAddress = httpAddress;
      servingInfo = new HashMap<String, Map<Long, Map<Integer, PartitionMetadata>>>();
    }

    public void addTablespaceVersionPartition(String tablespace, Long version, Integer partition) {
      addTablespaceVersionPartition(tablespace, version, partition, DEFAULT_METADATA);
    }

    public void addTablespaceVersionPartition(String tablespace, Long version, Integer partition, PartitionMetadata metadata) {
      if (servingInfo.get(tablespace) == null) {
        servingInfo.put(tablespace, new HashMap<Long, Map<Integer, PartitionMetadata>>());
      }
      if (servingInfo.get(tablespace).get(version) == null) {
        servingInfo.get(tablespace).put(version, new HashMap<Integer, PartitionMetadata>());
      }
      servingInfo.get(tablespace).get(version).put(partition, metadata);
    }

    public DNodeInfo getDNodeInfo() {
      return new DNodeInfo(address, httpAddress, servingInfo);
    }
  }

  /*
   * A concise way of asserting that a tablespace is valid
   * You must provide the list of dnodes in the order they would be found when iterating over the replication map
   * So if first partition (0) has two nodes and second partition (1) has one node you would provide:
   * shards = { 0, 0, 1 }, dnodes = { "node1", "node2", "node3" }
   */
  private static void assertTablespace(Tablespace tablespace, Integer[] shards, String... dnodes) {
    Set<Integer> uniqueShards = new HashSet<Integer>();
    uniqueShards.addAll(Arrays.asList(shards));
    assertEquals(uniqueShards.size(), tablespace.getPartitionMap().getPartitionEntries().size());
    assertEquals(uniqueShards.size(), tablespace.getReplicationMap().getReplicationEntries().size());
    int k = 0;
    for (int i = 0; i < uniqueShards.size(); i++) {
      ReplicationEntry entry = tablespace.getReplicationMap().getReplicationEntries().get(i);
      for (int j = 0; j < entry.getNodes().size(); j++, k++) {
        assertEquals(dnodes[k], entry.getNodes().get(j));
        assertEquals(shards[k], entry.getShard());
      }
    }
  }

  @Test
  public void testUpdateTablespacesExplicitEntryAndLeaving() throws TablespaceVersionInfoException {
    /**
     * In this test we are asserting that we can recreate the Partition and Replication map from individual DNodeInfo's.
     * This tests presents 3 DNodes that sum up 4 TablespaceVersions. One TablespaceVersion has a shard with replication = 2.
     * The other ones have one shard and two shards with rep = 1. The more variety, the more race conditions that can be found.
     *
     * We will add nodes one by one and assert that the code is commutative. So we will take one node down and enter it again
     * and things should be ok again. And then we will remove nodes one by one until we have an empty tablespaceversion map.
     */
    SploutConfiguration config = SploutConfiguration.getTestConfig();
    QNodeHandlerContext ctx = new QNodeHandlerContext(config, null);

    DNodeInfoFacade facade1 = new DNodeInfoFacade("dnode1");
    facade1.addTablespaceVersionPartition("t1", 1l, 0);
    facade1.addTablespaceVersionPartition("t1", 2l, 0);

    // DNode1 enters with partitions t1/1/0 and t1/2/0
    ctx.updateTablespaceVersions(facade1.getDNodeInfo(), DNodeEvent.ENTRY);

    assertEquals(2, ctx.getTablespaceVersionsMap().keySet().size());

    Tablespace tablespaceV1 = ctx.getTablespaceVersionsMap().get(new TablespaceVersion("t1", 1l));
    Tablespace tablespaceV2 = ctx.getTablespaceVersionsMap().get(new TablespaceVersion("t1", 2l));

    assertTablespace(tablespaceV1, new Integer[]{0}, "dnode1");
    assertTablespace(tablespaceV2, new Integer[]{0}, "dnode1");

    // DNode2 enters with partitions t1/1/0 and t1/2/1
    // DNode2 enters with partitions t1/3/0 and t2/1/0 too
    DNodeInfoFacade facade2 = new DNodeInfoFacade("dnode2");
    facade2.addTablespaceVersionPartition("t1", 1l, 0);
    facade2.addTablespaceVersionPartition("t1", 2l, 1);
    facade2.addTablespaceVersionPartition("t1", 3l, 0);
    facade2.addTablespaceVersionPartition("t2", 1l, 0);

    ctx.updateTablespaceVersions(facade2.getDNodeInfo(), DNodeEvent.ENTRY);

    Tablespace tablespace1V1 = ctx.getTablespaceVersionsMap().get(new TablespaceVersion("t1", 1l));
    Tablespace tablespace1V2 = ctx.getTablespaceVersionsMap().get(new TablespaceVersion("t1", 2l));
    Tablespace tablespace1V3 = ctx.getTablespaceVersionsMap().get(new TablespaceVersion("t1", 3l));
    Tablespace tablespace2V1 = ctx.getTablespaceVersionsMap().get(new TablespaceVersion("t2", 1l));

    assertTablespace(tablespace1V1, new Integer[]{0, 0}, "dnode1", "dnode2");
    assertTablespace(tablespace1V2, new Integer[]{0, 1}, "dnode1", "dnode2");
    assertTablespace(tablespace1V3, new Integer[]{0}, "dnode2");
    assertTablespace(tablespace2V1, new Integer[]{0}, "dnode2");

    // DNode3 enters with partitions t1/1/1 and t2/2/1
    DNodeInfoFacade facade3 = new DNodeInfoFacade("dnode3");
    facade3.addTablespaceVersionPartition("t1", 1l, 1);
    facade3.addTablespaceVersionPartition("t2", 1l, 1);

    ctx.updateTablespaceVersions(facade3.getDNodeInfo(), DNodeEvent.ENTRY);

    tablespace1V1 = ctx.getTablespaceVersionsMap().get(new TablespaceVersion("t1", 1l));
    tablespace1V2 = ctx.getTablespaceVersionsMap().get(new TablespaceVersion("t1", 2l));
    tablespace1V3 = ctx.getTablespaceVersionsMap().get(new TablespaceVersion("t1", 3l));
    tablespace2V1 = ctx.getTablespaceVersionsMap().get(new TablespaceVersion("t2", 1l));

    assertTablespace(tablespace1V1, new Integer[]{0, 0, 1}, "dnode1", "dnode2", "dnode3");
    assertTablespace(tablespace1V2, new Integer[]{0, 1}, "dnode1", "dnode2");
    assertTablespace(tablespace1V3, new Integer[]{0}, "dnode2");
    assertTablespace(tablespace2V1, new Integer[]{0, 1}, "dnode2", "dnode3");

    // DNode 2 leaves. Tablespace 1 version 3 becomes empty!
    ctx.updateTablespaceVersions(facade2.getDNodeInfo(), DNodeEvent.LEAVE);

    tablespace1V1 = ctx.getTablespaceVersionsMap().get(new TablespaceVersion("t1", 1l));
    tablespace1V2 = ctx.getTablespaceVersionsMap().get(new TablespaceVersion("t1", 2l));
    tablespace1V3 = ctx.getTablespaceVersionsMap().get(new TablespaceVersion("t1", 3l));
    tablespace2V1 = ctx.getTablespaceVersionsMap().get(new TablespaceVersion("t2", 1l));

    assertNull(tablespace1V3);
    assertTablespace(tablespace1V1, new Integer[]{0, 1}, "dnode1", "dnode3");
    assertTablespace(tablespace1V2, new Integer[]{0}, "dnode1");
    assertTablespace(tablespace2V1, new Integer[]{1}, "dnode3");

    // DNode 2 enters again. Assure that things remain the same as before.
    ctx.updateTablespaceVersions(facade2.getDNodeInfo(), DNodeEvent.ENTRY);

    tablespace1V1 = ctx.getTablespaceVersionsMap().get(new TablespaceVersion("t1", 1l));
    tablespace1V2 = ctx.getTablespaceVersionsMap().get(new TablespaceVersion("t1", 2l));
    tablespace1V3 = ctx.getTablespaceVersionsMap().get(new TablespaceVersion("t1", 3l));
    tablespace2V1 = ctx.getTablespaceVersionsMap().get(new TablespaceVersion("t2", 1l));

    assertTablespace(tablespace1V1, new Integer[]{0, 0, 1}, "dnode1", "dnode2", "dnode3");
    assertTablespace(tablespace1V2, new Integer[]{0, 1}, "dnode1", "dnode2");
    assertTablespace(tablespace1V3, new Integer[]{0}, "dnode2");
    assertTablespace(tablespace2V1, new Integer[]{0, 1}, "dnode2", "dnode3");

    // DNode 2 leaves. DNode 1 leaves.
    ctx.updateTablespaceVersions(facade2.getDNodeInfo(), DNodeEvent.LEAVE);
    ctx.updateTablespaceVersions(facade1.getDNodeInfo(), DNodeEvent.LEAVE);

    tablespace1V1 = ctx.getTablespaceVersionsMap().get(new TablespaceVersion("t1", 1l));
    tablespace1V2 = ctx.getTablespaceVersionsMap().get(new TablespaceVersion("t1", 2l));
    tablespace1V3 = ctx.getTablespaceVersionsMap().get(new TablespaceVersion("t1", 3l));
    tablespace2V1 = ctx.getTablespaceVersionsMap().get(new TablespaceVersion("t2", 1l));

    assertTablespace(tablespace1V1, new Integer[]{1}, "dnode3");
    assertNull(tablespace1V2);
    assertNull(tablespace1V3);
    assertTablespace(tablespace2V1, new Integer[]{1}, "dnode3");

    // DNode 3 leaves.
    ctx.updateTablespaceVersions(facade3.getDNodeInfo(), DNodeEvent.LEAVE);

    tablespace1V1 = ctx.getTablespaceVersionsMap().get(new TablespaceVersion("t1", 1l));
    tablespace1V2 = ctx.getTablespaceVersionsMap().get(new TablespaceVersion("t1", 2l));
    tablespace1V3 = ctx.getTablespaceVersionsMap().get(new TablespaceVersion("t1", 3l));
    tablespace2V1 = ctx.getTablespaceVersionsMap().get(new TablespaceVersion("t2", 1l));

    assertNull(tablespace1V1);
    assertNull(tablespace1V2);
    assertNull(tablespace1V3);
    assertNull(tablespace2V1);
  }

  @Test
  public void testUpdateTablespacesImplicitLeaving() throws TablespaceVersionInfoException {
    SploutConfiguration config = SploutConfiguration.getTestConfig();
    QNodeHandlerContext ctx = new QNodeHandlerContext(config, null);

    DNodeInfoFacade facade1 = new DNodeInfoFacade("dnode1");
    facade1.addTablespaceVersionPartition("t1", 1l, 0);
    facade1.addTablespaceVersionPartition("t1", 2l, 0);
    DNodeInfoFacade facade2 = new DNodeInfoFacade("dnode2");
    facade2.addTablespaceVersionPartition("t1", 1l, 1);
    facade2.addTablespaceVersionPartition("t1", 2l, 1);

    // DNode1 enters with partitions t1/1/0 and t1/2/0
    ctx.updateTablespaceVersions(facade1.getDNodeInfo(), DNodeEvent.ENTRY);
    // DNode2 enters with partitions t1/1/1 and t1/2/1
    ctx.updateTablespaceVersions(facade2.getDNodeInfo(), DNodeEvent.ENTRY);

    Tablespace tablespace1V1 = ctx.getTablespaceVersionsMap().get(new TablespaceVersion("t1", 1l));
    Tablespace tablespace1V2 = ctx.getTablespaceVersionsMap().get(new TablespaceVersion("t1", 2l));
    assertTablespace(tablespace1V1, new Integer[]{0, 1}, "dnode1", "dnode2");
    assertTablespace(tablespace1V2, new Integer[]{0, 1}, "dnode1", "dnode2");

    /**
     * Implicit leaving can happen in some cases like if one DNode removes an old version.
     * In this case it publishes new information that doesn't contain previous versions that it used to serve.
     */
    facade1 = new DNodeInfoFacade("dnode1");
    facade1.addTablespaceVersionPartition("t1", 1l, 0);

    // DNode1 : implicit leaving t1/2/0
    ctx.updateTablespaceVersions(facade1.getDNodeInfo(), DNodeEvent.UPDATE);

    tablespace1V1 = ctx.getTablespaceVersionsMap().get(new TablespaceVersion("t1", 1l));
    tablespace1V2 = ctx.getTablespaceVersionsMap().get(new TablespaceVersion("t1", 2l));
    assertTablespace(tablespace1V1, new Integer[]{0, 1}, "dnode1", "dnode2");
    assertTablespace(tablespace1V2, new Integer[]{1}, "dnode2");

    // DNode1 has the leaved version back again
    facade1.addTablespaceVersionPartition("t1", 2l, 0);
    ctx.updateTablespaceVersions(facade1.getDNodeInfo(), DNodeEvent.UPDATE);

    tablespace1V1 = ctx.getTablespaceVersionsMap().get(new TablespaceVersion("t1", 1l));
    tablespace1V2 = ctx.getTablespaceVersionsMap().get(new TablespaceVersion("t1", 2l));
    assertTablespace(tablespace1V1, new Integer[]{0, 1}, "dnode1", "dnode2");
    assertTablespace(tablespace1V2, new Integer[]{0, 1}, "dnode1", "dnode2");

    // DNode1, DNode2 leave everything implicitly
    facade1 = new DNodeInfoFacade("dnode1");
    facade2 = new DNodeInfoFacade("dnode2");

    ctx.updateTablespaceVersions(facade1.getDNodeInfo(), DNodeEvent.UPDATE);
    ctx.updateTablespaceVersions(facade2.getDNodeInfo(), DNodeEvent.UPDATE);

    tablespace1V1 = ctx.getTablespaceVersionsMap().get(new TablespaceVersion("t1", 1l));
    tablespace1V2 = ctx.getTablespaceVersionsMap().get(new TablespaceVersion("t1", 2l));
    assertNull(tablespace1V1);
    assertNull(tablespace1V2);
  }
}
