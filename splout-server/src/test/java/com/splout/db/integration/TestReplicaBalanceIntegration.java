package com.splout.db.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Test;

import com.google.common.io.Files;
import com.splout.db.common.ReplicationEntry;
import com.splout.db.common.ReplicationMap;
import com.splout.db.common.SploutClient;
import com.splout.db.common.Tablespace;
import com.splout.db.common.TestUtils;
import com.splout.db.dnode.DNode;
import com.splout.db.dnode.TestCommands;

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
						if(clients[i].dNodeList().size() != 3) {
							return false;
						}
					}
					return true;
				} catch(IOException e) {
					// test failed
					e.printStackTrace();
					return true;
				}
			}
		}.waitAtMost(5000);
		
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
		}.waitAtMost(5000);

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
		}.waitAtMost(5000);

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
		}.waitAtMost(5000);

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
		}.waitAtMost(5000);

		assertEquals(2, partitionsByNode1.size());
		assertTrue(partitionsByNode1.contains(0));
		assertTrue(partitionsByNode1.contains(1));
	}
}
