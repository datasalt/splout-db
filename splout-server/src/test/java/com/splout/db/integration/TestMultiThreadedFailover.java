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

import java.io.File;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.splout.db.common.SploutClient;
import com.splout.db.common.Tablespace;
import com.splout.db.dnode.TestCommands;
import com.splout.db.engine.SQLite4JavaClient;
import com.splout.db.qnode.QNode;
import com.splout.db.qnode.beans.QueryStatus;
import com.splout.db.thrift.DNodeException;

/**
 * Similar to {@link TestMultiThreadedQueryAndDeploy} but it only performs one deploy at the beginning and then it queries
 * and at the same time brings some dNodes down and up... This is for checking parallel robustness of fail-over.
 */
public class TestMultiThreadedFailover extends BaseIntegrationTest {

	private final static Logger log = LoggerFactory.getLogger(TestMultiThreadedFailover.class);

	public final static int N_QNODES = 1;
	public final static int N_DNODES = 2;
	public final static int N_THREADS = 10;
	public final static String TABLESPACE = "t1";
	public final static String TABLE = "foo";

	public final static long SEED = 12345678;
	public final static String TMP_FOLDER = "tmp-" + TestMultiThreadedFailover.class.getName();

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

		deployIteration(0, random, client, testTablespace);
		
		for(QNode qnode : getqNodes()) {
			// Make sure all QNodes are aware of the the first deploy
			// There might be some delay as they have to receive notifications via Hazelcast etc
			long waitedSoFar = 0;
			QueryStatus status = null;
			SploutClient perQNodeClient = new SploutClient(qnode.getAddress());
			do {
				status = perQNodeClient.query(TABLESPACE, "0", "SELECT * FROM " + TABLE + ";", null);
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

			// This is the "mother-fucker" thread.
			// It will bring DNodes down on purpose.
			// And then bring them up again.
			service.submit(new Runnable() {

				@Override
				public void run() {

					while(true) {
						try {
							Thread.sleep(1000);
							log.info("Time to kill some DNode...");
							int whichOne = (int) (Math.random() * getdNodes().size());
							getdNodes().get(whichOne).testCommand(TestCommands.SHUTDOWN.toString());
							Thread.sleep(1000);
							log.info("Time to bring the DNode back to life...");
							getdNodes().get(whichOne).testCommand(TestCommands.RESTART.toString());
						} catch(InterruptedException e) {
							log.info("MFT - Bye bye!");
						} catch(DNodeException e) {
							failed.set(true);
							e.printStackTrace();
							throw new RuntimeException(e);
						} catch(TException e) {
							failed.set(true);
							e.printStackTrace();
							throw new RuntimeException(e);
						}
					}
				}

			});

			// These threads will continuously perform queries and check that the results are consistent.
			for(int i = 0; i < N_THREADS; i++) {
				service.submit(new Runnable() {
					@SuppressWarnings("unchecked")
          @Override
					public void run() {
						try {
							while(true) {
								int randomDNode = Math.abs(random.nextInt()) % N_DNODES;
								QueryStatus status = client.query(TABLESPACE, ((randomDNode * 10) - 1) + "", "SELECT * FROM "
								    + TABLE + ";", null);
								log.info("Query status -> " + status);
								assertEquals(1, status.getResult().size());
								Map<String, Object> jsonResult = (Map<String, Object>) status.getResult().get(0);
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
			
			Thread.sleep(15000);
			
			assertEquals(false, failed.get());
			
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
			SQLite4JavaClient manager = new SQLite4JavaClient(dbData + "", null, false, 0);
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
