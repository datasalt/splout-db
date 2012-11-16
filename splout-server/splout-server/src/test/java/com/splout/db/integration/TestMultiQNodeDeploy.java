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

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import com.splout.db.common.SQLiteJDBCManager;
import com.splout.db.common.SploutClient;
import com.splout.db.common.SploutConfiguration;
import com.splout.db.common.Tablespace;
import com.splout.db.qnode.beans.QueryStatus;

/**
 * This is a kind of integration test that tries to find bugs that will only appear when there are more than one QNode
 * handling deploys, queries, etc. The test instantiates M Qnodes, N Dnodes and deploys several versions of the same
 * tablespace. After each deployment we query for checking that the expected version is there. We use a Random object
 * with a deterministic SEED.
 * <p>
 * TIP: Use "BaseIntegrationTest" for creating new integration tests.
 */
public class TestMultiQNodeDeploy extends BaseIntegrationTest {

	public final static int N_QNODES = 2;
	public final static int N_DNODES = 3;

	public final static long SEED = 12345678;
	public final static String TMP_FOLDER = "tmp-" + TestMultiQNodeDeploy.class.getName();

	@Test
	public void test() throws Throwable {
		FileUtils.deleteDirectory(new File(TMP_FOLDER));
		new File(TMP_FOLDER).mkdirs();

		createSploutEnsemble(N_QNODES, N_DNODES);
		Random random = new Random(SEED);

		try {
			for(int i = 0; i < 3; i++) {
				deployAndQueryRandomTablespace(random);
			}
		} finally {
			closeSploutEnsemble();
			FileUtils.deleteDirectory(new File(TMP_FOLDER));
		}
	}

	private SploutClient getRandomQNodeClient(Random random, SploutConfiguration config) {
		int chosenQnode = Math.abs(random.nextInt()) % N_QNODES;
		return new SploutClient(qNodes.get(chosenQnode).getAddress());
	}

	@SuppressWarnings("unchecked")
	private void deployAndQueryRandomTablespace(Random random) throws Exception {
		Tablespace testTablespace = createTestTablespace(N_DNODES);

		File deployData = new File(TMP_FOLDER + "/" + "deploy-folder-" + random.nextInt());
		deployData.mkdir();

		// Each random deployment will have a fixed random string associated with it
		String randomStr = "ID" + Math.abs(random.nextInt());

		for(int i = 0; i < N_DNODES; i++) {
			File dbData = new File(deployData, i + ".db");
			SQLiteJDBCManager manager = new SQLiteJDBCManager(dbData + "", 10);
			// We create a foo database with one integer and one text
			manager.query("CREATE TABLE foo (intCol INT, strCol TEXT);", 100);
			// We insert as many values as the ones we defined in the partition map
			for(int j = i * 10; j < (i * 10 + 10); j++) {
				manager.query("INSERT INTO foo VALUES (" + j + ", " + "'" + randomStr + "');", 100);
			}
			manager.close();
		}

		SploutConfiguration config = SploutConfiguration.getTestConfig();
		SploutClient client = getRandomQNodeClient(random, config);
		client.deploy("p1", testTablespace.getPartitionMap(), testTablespace.getReplicationMap(),
		    deployData.getAbsoluteFile().toURI());

		Thread.sleep(1000); // TODO How to improve this.

		// Perform N queries, one to each DNode and validate the resultant data
		for(int i = 0; i < N_DNODES; i++) {
			client = getRandomQNodeClient(random, config);
			QueryStatus qStatus = client.query("p1", i * 10 + "", "SELECT * FROM foo;");
			assertEquals((Integer) i, qStatus.getShard());
			assertEquals(10, qStatus.getResult().size());
			for(Object obj : qStatus.getResult()) {
				Map<String, Object> map = (Map<String, Object>) obj;
				assertEquals(randomStr, map.get("strCol"));
			}
		}
	}
}
