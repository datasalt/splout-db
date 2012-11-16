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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Test;

import com.hazelcast.core.Hazelcast;
import com.splout.db.common.JSONSerDe;
import com.splout.db.common.JSONSerDe.JSONSerDeException;
import com.splout.db.common.PartitionEntry;
import com.splout.db.common.PartitionMap;
import com.splout.db.common.ReplicationEntry;
import com.splout.db.common.ReplicationMap;
import com.splout.db.common.SploutConfiguration;
import com.splout.db.common.Tablespace;
import com.splout.db.common.TestUtils;
import com.splout.db.dnode.DNode;
import com.splout.db.dnode.DNodeMockHandler;
import com.splout.db.dnode.DNodeProperties;
import com.splout.db.hazelcast.TablespaceVersion;
import com.splout.db.qnode.beans.QueryStatus;
import com.splout.db.thrift.DNodeException;

public class TestMultiQuery {

	@After
	public void cleanUp() throws IOException {
		TestUtils.cleanUpTmpFolders(this.getClass().getName(), 4);
	}

	static class TellIDHandler extends DNodeMockHandler {

		private String id;

		public TellIDHandler(String id) {
			this.id = id;
		}

		/*
		 * This handler, when queried, returns its own ID
		 */
		@Override
		public String sqlQuery(String tablespace, long version, int partition, String query) throws DNodeException {
			try {
				return JSONSerDe.ser(Arrays.asList(new String[] { id }));
			} catch(JSONSerDeException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}
	};

	@Test
	public void test() throws Throwable {
		QNodeHandler handler = new QNodeHandler();
		handler.init(SploutConfiguration.getTestConfig());

		List<DNode> dNodes = new ArrayList<DNode>();

		try {
			// Create an ensemble of 5 DNodes
			List<PartitionEntry> partitions = new ArrayList<PartitionEntry>();
			List<ReplicationEntry> replicationEntries = new ArrayList<ReplicationEntry>();
			for(int i = 0; i < 5; i++) {
				SploutConfiguration dNodeConfig = SploutConfiguration.getTestConfig();
				// We instantiate a DNode with a handler that returns "DNode" + i for any query
				DNode dnode = TestUtils.getTestDNode(dNodeConfig, new TellIDHandler("DNode" + i), "dnode-"
				    + this.getClass().getName() + "-" + i);
				dNodes.add(dnode);
				// Define the partition for this DNode
				// DNode 0 will have [10, 20), DNode 1 [20, 30], ...
				PartitionEntry partitionEntry = new PartitionEntry();
				partitionEntry.setMin((i * 10 + 10) + "");
				partitionEntry.setMax((i * 10 + 20) + "");
				partitionEntry.setShard(i);
				partitions.add(partitionEntry);
				// And the replication
				ReplicationEntry repEntry = new ReplicationEntry();
				repEntry.setShard(i);
				repEntry.setNodes(Arrays.asList(new String[] { "localhost:" + dNodeConfig.getInt(DNodeProperties.PORT) }));
				replicationEntries.add(repEntry);
			}

			Tablespace tablespace1 = new Tablespace(new PartitionMap(partitions), new ReplicationMap(replicationEntries), 1l,
			    0l);
			handler.getContext().getTablespaceVersionsMap().put(new TablespaceVersion("tablespace1", 1l), tablespace1);
			handler.getContext().getCurrentVersionsMap().put("tablespace1", 1l);

			// The following has to be read as: multi-query range [23-25)
			// The impacted shard will be 1 [20-30)
			List<String> keyMins = Arrays.asList(new String[] { "23" });
			List<String> keyMaxs = Arrays.asList(new String[] { "25" });

			ArrayList<QueryStatus> resultObj = handler.multiQuery("tablespace1", keyMins, keyMaxs, "SELECT 1;");
			assertEquals(1, resultObj.size());

			assertTrue(1 == resultObj.get(0).getShard());
			assertEquals("DNode1", resultObj.get(0).getResult().get(0));

			// The following has to be read as: multi-query [13-25) Union [45-55).
			// The impacted shards will be 0 [10-20), 1 [20, 30), 3 [40, 50), 5 [50, 60)
			keyMins = Arrays.asList(new String[] { "13", "45" });
			keyMaxs = Arrays.asList(new String[] { "25", "55" });

			resultObj = handler.multiQuery("tablespace1", keyMins, keyMaxs, "SELECT 1;");

			assertEquals(4, resultObj.size());

			// The impacted shards will be 0 [0-10), 1 [10, 20), 3 [30, 40), 4 [40, 50)
			assertTrue(0 == resultObj.get(0).getShard());
			assertTrue(1 == resultObj.get(1).getShard());
			assertTrue(3 == resultObj.get(2).getShard());
			assertTrue(4 == resultObj.get(3).getShard());

			assertEquals("DNode0", resultObj.get(0).getResult().get(0));
			assertEquals("DNode1", resultObj.get(1).getResult().get(0));
			assertEquals("DNode3", resultObj.get(2).getResult().get(0));
			assertEquals("DNode4", resultObj.get(3).getResult().get(0));

			// The following has to be read as: multi-query the opened range (-Infinity, Infinity) regardless of the key type.
			// The impacted shards will be all: 0, 1, 2, 3, 4
			keyMins = Arrays.asList(new String[] {});
			keyMaxs = Arrays.asList(new String[] {});

			resultObj = handler.multiQuery("tablespace1", keyMins, keyMaxs, "SELECT 1;");

			assertEquals(5, resultObj.size());

			for(int i = 0; i < 5; i++) {
				assertTrue(i == resultObj.get(i).getShard());
				assertEquals("DNode" + i, resultObj.get(i).getResult().get(0));
			}
		} finally {
			handler.close();
			for(DNode dnode : dNodes) {
				dnode.stop();
			}
			Hazelcast.shutdownAll();
		}
	}
}
