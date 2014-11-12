package com.splout.db.integration;

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

import com.google.common.io.Files;
import com.splout.db.common.*;
import com.splout.db.dnode.DNode;
import com.splout.db.dnode.TestCommands;
import com.splout.db.qnode.QNodeHandler;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestReplicaBalanceIntegration extends BaseIntegrationTest {

	public final static int N_QNODES = 3;
	public final static int N_DNODES = 3;

	public final static long SEED = 12345678;
	public final static String TMP_FOLDER = "tmp-" + TestReplicaBalanceIntegration.class.getName();

	@After
	public void cleanTestBinaryFiles() throws IOException {
		FileUtils.deleteDirectory(new File(TMP_FOLDER));
	}

	@Test
	public void test() throws Throwable {
		FileUtils.deleteDirectory(new File(TMP_FOLDER));
		new File(TMP_FOLDER).mkdirs();

		createSploutEnsemble(N_QNODES, N_DNODES);
		Random random = new Random(SEED);

		Tablespace testTablespace = createTestTablespace(N_DNODES);

		File deployData = new File(TMP_FOLDER + "/" + "deploy-folder-" + random.nextInt());
		deployData.mkdir();

		for(int i = 0; i < N_DNODES; i++) {
			File dbData = new File(deployData, i + ".db");
			Files.write(new String("foo").getBytes(), dbData);
		}

		final SploutClient[] clients = new SploutClient[N_QNODES];

		for(int i = 0; i < N_QNODES; i++) {
			clients[i] = new SploutClient(getqNodes().get(i).getAddress());
		}
		final SploutClient client1 = clients[0];

		// Check that all QNodes have the full list of DNodes
		new TestUtils.NotWaitingForeverCondition() {
			@Override
			public boolean endCondition() {
				try {
					for(int i = 0; i < N_QNODES; i++) {
						List<String> dNodeList = clients[i].dNodeList();
						if(dNodeList.size() != 3) {
							return false;
						}
						QNodeHandler handler = (QNodeHandler)getqNodes().get(i).getHandler();
						for(String dnode: dNodeList) {
							if(handler.getContext().getThriftClientCache().get(dnode) == null) {
								return false;
							}
						}
					}
					return true;
				} catch(IOException e) {
					// test failed
					e.printStackTrace();
					return true;
				}
			}
		}.waitAtMost(20000);
		
		// Deploy
		client1.deploy("p1", testTablespace.getPartitionMap(), testTablespace.getReplicationMap(),
		    deployData.getAbsoluteFile().toURI());

		// Check that all QNodes have the deployment data
		new TestUtils.NotWaitingForeverCondition() {

			@Override
			public boolean endCondition() {
				try {
					for(int i = 0; i < N_QNODES; i++) {
						Map<String, Tablespace> t = clients[i].overview().getTablespaceMap();
						if(t.size() != 1) {
							return false;
						}
						for(int k = 0; k < 3; k++) {
							if(t.get("p1").getReplicationMap().getReplicationEntries().get(k).getNodes().size() != 2) {
								return false;
							}
						}
					}
					return true;
				} catch(IOException e) {
					// test failed
					e.printStackTrace();
					return true;
				}
			}
		}.waitAtMost(20000);

		final DNode dnode1 = getdNodes().get(1);

		final Set<Integer> partitionsByNode1 = new HashSet<Integer>();
		partitionsByNode1.add(0);
		partitionsByNode1.add(1);

		// shutdown DNode1 and see what happens with auto-rebalancing
		// the "partitionsByNode1" will become under-replicated and after a short period of time should be balanced
		dnode1.testCommand(TestCommands.SHUTDOWN.toString());

		// waiting until the system becomes under-replicated
		new TestUtils.NotWaitingForeverCondition() {

			@Override
			public boolean endCondition() {
				try {
					boolean dnode1NotPresent = true;
					for(int i = 0; i < N_QNODES; i++) {
						Map.Entry<String, Tablespace> tEntry = clients[i].overview().getTablespaceMap().entrySet()
						    .iterator().next();
						ReplicationMap currentReplicationMap = tEntry.getValue().getReplicationMap();
						for(ReplicationEntry entry : currentReplicationMap.getReplicationEntries()) {
							if(entry.getNodes().contains(dnode1.getAddress())) {
								dnode1NotPresent = false;
							}
						}
					}
					return dnode1NotPresent;
				} catch(IOException e) {
					// test failed
					e.printStackTrace();
					return true;
				}
			}
		}.waitAtMost(60000);

		// waiting now until the system recovers itself without dnode1
		new TestUtils.NotWaitingForeverCondition() {

			@Override
			public boolean endCondition() {
				try {
					boolean balanced = true;
					for(int i = 0; i < N_QNODES; i++) {
						Map.Entry<String, Tablespace> tEntry = clients[i].overview().getTablespaceMap().entrySet()
						    .iterator().next();
						ReplicationMap currentReplicationMap = tEntry.getValue().getReplicationMap();
						for(ReplicationEntry entry : currentReplicationMap.getReplicationEntries()) {
							if(entry.getNodes().size() < entry.getExpectedReplicationFactor()) {
								balanced = false;
							}
						}
					}
					return balanced;
				} catch(IOException e) {
					// test failed
					e.printStackTrace();
					return true;
				}
			}
		}.waitAtMost(20000);

		// now we bring back dnode1 to life
		// what will happen now is that the partitions it seves will be over-replicated
		dnode1.testCommand(TestCommands.RESTART.toString());

		// waiting now until the system is over-replicated
		new TestUtils.NotWaitingForeverCondition() {

			@Override
			public boolean endCondition() {
				try {
					boolean overreplicated = true;
					for(int i = 0; i < N_QNODES; i++) {
						Map.Entry<String, Tablespace> tEntry = clients[i].overview().getTablespaceMap().entrySet()
						    .iterator().next();
						ReplicationMap currentReplicationMap = tEntry.getValue().getReplicationMap();
						for(ReplicationEntry entry : currentReplicationMap.getReplicationEntries()) {
							if(partitionsByNode1.contains(entry.getShard())
							    && entry.getNodes().size() <= entry.getExpectedReplicationFactor()) {
								overreplicated = false;
							}
						}
					}
					return overreplicated;
				} catch(IOException e) {
					// test failed
					e.printStackTrace();
					return true;
				}
			}
		}.waitAtMost(20000);

		assertEquals(2, partitionsByNode1.size());
		assertTrue(partitionsByNode1.contains(0));
		assertTrue(partitionsByNode1.contains(1));
	}
}
