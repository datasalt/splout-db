package com.splout.db.qnode;

/*
 * #%L
 * Splout SQL Server
 * %%
 * Copyright (C) 2012 - 2013 Datasalt Systems S.L.
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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.hazelcast.core.IMap;
import com.splout.db.common.SploutConfiguration;
import com.splout.db.hazelcast.CoordinationStructures;
import com.splout.db.hazelcast.DNodeInfo;
import com.splout.db.qnode.QNodeHandlerContext.DNodeEvent;
import com.splout.db.qnode.QNodeHandlerContext.TablespaceVersionInfoException;
import com.splout.db.qnode.TestQNodeHandlerContext.DNodeInfoFacade;
import com.splout.db.thrift.PartitionMetadata;

public class TestReplicaBalancer {

	public static class CoordinationStructuresMock extends CoordinationStructures {

		List<String> dNodes;
		List<DNodeInfo> dNodeInfo;

		public CoordinationStructuresMock(List<String> dNodes, List<DNodeInfo> dNodeInfo) {
			super(null);
			this.dNodes = dNodes;
			this.dNodeInfo = dNodeInfo;
		}

		@SuppressWarnings("unchecked")
    @Override
		public IMap<String, DNodeInfo> getDNodes() {
			return new FixedDNodeList(new HashSet<String>(dNodes), dNodeInfo);
		}
	}

	@Test
	public void test() throws TablespaceVersionInfoException {

		SploutConfiguration config = SploutConfiguration.getTestConfig();

		PartitionMetadata metadata3Replicas = new PartitionMetadata();
		metadata3Replicas.setNReplicas(3);
		PartitionMetadata metadata1Replicas = new PartitionMetadata();
		metadata1Replicas.setNReplicas(1);

		// t1, version 1, partition 0 -> 3 replicas, only 2 dnodes: dnode1, dnode2
		DNodeInfoFacade facade1 = new DNodeInfoFacade("dnode1");
		facade1.addTablespaceVersionPartition("t1", 1l, 0, metadata3Replicas);
		DNodeInfoFacade facade2 = new DNodeInfoFacade("dnode2");
		// t2, version 1, partition 0 -> 1 replica, 1 dnode: dnode3
		facade2.addTablespaceVersionPartition("t1", 1l, 0, metadata3Replicas);
		DNodeInfoFacade facade3 = new DNodeInfoFacade("dnode3");

		facade3.addTablespaceVersionPartition("t2", 1l, 0, metadata1Replicas);

		List<String> dnodes = Arrays.asList(new String[] { "dnode1", "dnode2", "dnode3" });
		List<DNodeInfo> dNodeInfo = Arrays.asList(new DNodeInfo[] { facade1.getDNodeInfo(),
		    facade2.getDNodeInfo(), facade3.getDNodeInfo() });

		QNodeHandlerContext ctx = new QNodeHandlerContext(config, new CoordinationStructuresMock(dnodes,
		    dNodeInfo));
		ctx.updateTablespaceVersions(dNodeInfo.get(0), DNodeEvent.ENTRY);
		ctx.updateTablespaceVersions(dNodeInfo.get(1), DNodeEvent.ENTRY);
		ctx.updateTablespaceVersions(dNodeInfo.get(2), DNodeEvent.ENTRY);

		ReplicaBalancer balancer = new ReplicaBalancer(ctx);
		List<ReplicaBalancer.BalanceAction> balanceActions = balancer.scanPartitions();

		Assert.assertEquals(1, balanceActions.size());
		Assert.assertEquals("dnode3", balanceActions.get(0).getFinalNode());
		Assert.assertEquals("t1", balanceActions.get(0).getTablespace());
		Assert.assertEquals(0, balanceActions.get(0).getPartition());
		Assert.assertEquals(1l, balanceActions.get(0).getVersion());

		Assert.assertTrue("dnode1".equals(balanceActions.get(0).getOriginNode())
		    || "dnode2".equals(balanceActions.get(0).getOriginNode()));
	}
}
