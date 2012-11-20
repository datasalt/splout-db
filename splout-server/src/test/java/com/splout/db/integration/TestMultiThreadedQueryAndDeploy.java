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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.splout.db.common.SQLiteJDBCManager;
import com.splout.db.common.SploutClient;
import com.splout.db.common.SploutConfiguration;
import com.splout.db.common.Tablespace;
import com.splout.db.common.TestUtils;
import com.splout.db.hazelcast.CoordinationStructures;
import com.splout.db.hazelcast.TablespaceVersion;
import com.splout.db.qnode.QNode;
import com.splout.db.qnode.QNodeHandler;
import com.splout.db.qnode.QNodeProperties;
import com.splout.db.qnode.beans.QueryStatus;

/**
 * This integration test is similar to "MultiQNodeTest" but it uses threads for querying the QNodes meanwhile deploys
 * are performed asynchronously. Here we are asserting that Splout is robust when deploying and serving queries in
 * parallel. We will also assert that after a lot of deployments there are only {@link
 * com.splout.db.qnode.QNodeProperties.#VERSIONS_PER_TABLESPACE} versions left and therefore the cleaning of versions
 * works well on-the-fly.
 * <p>
 * TIP: Use "BaseIntegrationTest" for creating new integration tests.
 */
@SuppressWarnings("unchecked")
public class TestMultiThreadedQueryAndDeploy extends BaseIntegrationTest {

	private final static Logger log = LoggerFactory.getLogger(TestMultiThreadedQueryAndDeploy.class);

	public final static int N_QNODES = 3;
	public final static int N_DNODES = 4;
	public final static int N_THREADS = 10;
	public final static String TABLESPACE = "t1";
	public final static String TABLE = "foo";

	public final static long SEED = 12345678;
	public final static String TMP_FOLDER = "tmp-" + TestMultiThreadedQueryAndDeploy.class.getName();

	@Test
	public void test() throws Throwable {
		FileUtils.deleteDirectory(new File(TMP_FOLDER));
		new File(TMP_FOLDER).mkdirs();

		createSploutEnsemble(N_QNODES, N_DNODES);
		String[] qNodeAddresses = new String[N_QNODES];
		for(int i = 0; i < N_QNODES; i++) {
			qNodeAddresses[i] = getqNodes().get(i).getAddress();
		}

		final SploutClient client = new SploutClient(qNodeAddresses);
		final Tablespace testTablespace = createTestTablespace(N_DNODES);
		final Random random = new Random(SEED);
		final AtomicBoolean failed = new AtomicBoolean(false);
		final AtomicInteger iteration = new AtomicInteger(0);
		final Set<Integer> iterationsSeen = new HashSet<Integer>();

		deployIteration(0, random, client, testTablespace);

		for(QNode qnode : getqNodes()) {
			// Make sure all QNodes are aware of the the first deploy
			// There might be some delay as they have to receive notifications via Hazelcast etc
			long waitedSoFar = 0;
			QueryStatus status = null;
			SploutClient perQNodeClient = new SploutClient(qnode.getAddress());
			do {
				status = perQNodeClient.query(TABLESPACE, "0", "SELECT * FROM " + TABLE + ";");
				Thread.sleep(100);
				waitedSoFar += 100;
				if(waitedSoFar > 5000) {
					throw new AssertionError("Waiting too much on a test condition");
				}
			} while(status == null || status.getError() != null);
			log.info("QNode [" + qnode.getAddress() + "] is ready to serve deploy 0.");
		}

		try {
			// Business logic here
			ExecutorService service = Executors.newFixedThreadPool(N_THREADS);

			// These threads will continuously perform queries and check that the results is consistent.
			// They will also count how many deploys have happened since the beginning.
			for(int i = 0; i < N_THREADS; i++) {
				service.submit(new Runnable() {
					@Override
					public void run() {
						try {
							while(true) {
								int randomDNode = Math.abs(random.nextInt()) % N_DNODES;
								QueryStatus status = client.query(TABLESPACE, (randomDNode * 10) + "", "SELECT * FROM "
								    + TABLE + ";");
								log.info("Query status -> " + status);
								assertEquals(1, status.getResult().size());
								Map<String, Object> jsonResult = (Map<String, Object>) status.getResult().get(0);
								Integer seenIteration = (Integer) jsonResult.get("iteration");
								synchronized(iterationsSeen) {
									iterationsSeen.add(seenIteration);
								}
								assertTrue(seenIteration <= iteration.get());
								assertEquals(randomDNode, jsonResult.get("dnode"));
								Thread.sleep(100);
							}
						} catch(InterruptedException ie) {
							// Bye bye
							log.info("Bye bye!");
						} catch(Throwable e) {
							e.printStackTrace();
							failed.set(true);
						}
					}
				});
			}

			final SploutConfiguration config = SploutConfiguration.getTestConfig();
			final int iterationsToPerform = config.getInt(QNodeProperties.VERSIONS_PER_TABLESPACE) + 5;
			for(int i = 0; i < iterationsToPerform; i++) {
				iteration.incrementAndGet();
				log.info("Deploy iteration: " + iteration.get());
				deployIteration(iteration.get(), random, client, testTablespace);

				new TestUtils.NotWaitingForeverCondition() {
					@Override
					public boolean endCondition() {
						synchronized(iterationsSeen) {
							return iterationsSeen.size() == (iteration.get() + 1);
						}
					}
				}.waitAtMost(5000);
			}

			assertEquals(false, failed.get());

			service.shutdownNow(); // will interrupt all threads
			while(!service.isTerminated()) {
				Thread.sleep(100);
			}

			CoordinationStructures coord = TestUtils.getCoordinationStructures(config);
			assertNotNull(coord.getCopyVersionsBeingServed().get(TABLESPACE));

			// Assert that there is only MAX_VERSIONS versions of the tablespace (due to old version cleanup)
			new TestUtils.NotWaitingForeverCondition() {

				@Override
				public boolean endCondition() {
					QNodeHandler handler = (QNodeHandler) qNodes.get(0).getHandler();
					int seenVersions = 0;
					for(Map.Entry<TablespaceVersion, Tablespace> tablespaceVersion : handler.getContext()
					    .getTablespaceVersionsMap().entrySet()) {
						if(tablespaceVersion.getKey().getTablespace().equals(TABLESPACE)) {
							seenVersions++;
						}
					}
					return seenVersions <= config.getInt(QNodeProperties.VERSIONS_PER_TABLESPACE);
				}
			}.waitAtMost(5000);
		} finally {
			closeSploutEnsemble();
			FileUtils.deleteDirectory(new File(TMP_FOLDER));
		}
	}

	private void deployIteration(int iteration, Random random, SploutClient client,
	    Tablespace testTablespace) throws Exception {
		File deployData = new File(TMP_FOLDER + "/" + "deploy-folder-" + random.nextInt());
		deployData.mkdir();

		for(int i = 0; i < N_DNODES; i++) {
			File dbData = new File(deployData, i + ".db");
			SQLiteJDBCManager manager = new SQLiteJDBCManager(dbData + "", 10);
			// We create a foo database with one integer and one text
			manager.query("CREATE TABLE " + TABLE + " (iteration INT, dnode INT);", 100);
			// We insert as many values as the ones we defined in the partition map
			manager.query("INSERT INTO " + TABLE + " VALUES (" + iteration + ", " + i + ");", 100);
			manager.close();
		}

		log.info("Deploying deploy iteration [" + iteration + "]");
		client.deploy(TABLESPACE, testTablespace.getPartitionMap(), testTablespace.getReplicationMap(),
		    deployData.getAbsoluteFile().toURI());
	}
}
