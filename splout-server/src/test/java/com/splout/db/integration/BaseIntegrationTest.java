package com.splout.db.integration;

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
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.Before;

import com.hazelcast.core.Hazelcast;
import com.splout.db.common.PartitionEntry;
import com.splout.db.common.PartitionMap;
import com.splout.db.common.ReplicationEntry;
import com.splout.db.common.ReplicationMap;
import com.splout.db.common.SploutClient;
import com.splout.db.common.SploutConfiguration;
import com.splout.db.common.Tablespace;
import com.splout.db.common.TestUtils;
import com.splout.db.dnode.DNode;
import com.splout.db.dnode.DNodeHandler;
import com.splout.db.dnode.DNodeProperties;
import com.splout.db.qnode.QNode;
import com.splout.db.qnode.QNodeHandler;

public class BaseIntegrationTest {
	
	@After
	@Before
	public void cleanUp() throws IOException {
		TestUtils.cleanUpTmpFolders(this.getClass().getName(), 4);
	}
	
	List<DNode> dNodes = new ArrayList<DNode>();
	List<QNode> qNodes = new ArrayList<QNode>();
	
	protected SploutClient getRandomQNodeClient(Random random, SploutConfiguration config) {
		int chosenQnode = Math.abs(random.nextInt()) % qNodes.size();
		return new SploutClient(qNodes.get(chosenQnode).getAddress());
	}

	public void createSploutEnsemble(int nQnodes, int nDnodes) throws Throwable {
		/*
		 * Create an ensemble of QNodes
		 */
		for(int i = 0; i < nQnodes; i++) {
			SploutConfiguration qNodeConfig = SploutConfiguration.getTestConfig();
			QNode qnode = TestUtils.getTestQNode(qNodeConfig, new QNodeHandler());
			qNodes.add(qnode);
		}

		/*
		 * Create an ensemble of DNodes
		 */
		for(int i = 0; i < nDnodes; i++) {
			SploutConfiguration dNodeConfig = SploutConfiguration.getTestConfig();
			dNodeConfig.setProperty(DNodeProperties.HANDLE_TEST_COMMANDS, true);
			DNode dnode = TestUtils.getTestDNode(dNodeConfig, new DNodeHandler(), "dnode-" + this.getClass().getName() + "-" + i);
			dNodes.add(dnode);
		}
	}

	public void closeSploutEnsemble() throws Exception {
		for(QNode qnode : qNodes) {
			qnode.close();
		}
		for(DNode dnode : dNodes) {
			dnode.stop();
		}
		Hazelcast.shutdownAll();
	}

	/**
	 * Creates an evenly-distributed, integer based partition map for a fixed number of DNodes. The separation is 10, so
	 * Dnode 0 will have keys [0, 10), Dnode 1 will have keys [10, 20) and so on.
	 */
	public Tablespace createTestTablespace(int nDnodes) {
		List<PartitionEntry> partitions = new ArrayList<PartitionEntry>();
		List<ReplicationEntry> replicationEntries = new ArrayList<ReplicationEntry>();
		for(int i = 0; i < nDnodes; i++) {
			// Define the partition for this DNode
			// DNode 0 will have [0, 10], DNode 1 [10, 20], ...
			PartitionEntry partitionEntry = new PartitionEntry();
			partitionEntry.setMin((i * 10) + "");
			partitionEntry.setMax((i * 10 + 10) + "");
			partitionEntry.setShard(i);
			partitions.add(partitionEntry);
			// And the replication
			ReplicationEntry repEntry = new ReplicationEntry();
			repEntry.setShard(i);
			int j = i + 1;
			if(j == nDnodes) {
				j = 0;
			}
			repEntry.setNodes(Arrays.asList(new String[] { dNodes.get(i).getAddress(), dNodes.get(j).getAddress() }));
			replicationEntries.add(repEntry);
		}
		// A valid partition map is complete: first min is null and last max is null.
		partitions.get(0).setMin(null);
		partitions.get(partitions.size() - 1).setMax(null);
		return new Tablespace(new PartitionMap(partitions), new ReplicationMap(replicationEntries), 0l, 0l);
	}
	
	// ---- Getters ---- //
	
	public List<DNode> getdNodes() {
		return dNodes;
	}

	public List<QNode> getqNodes() {
		return qNodes;
	}	
}
