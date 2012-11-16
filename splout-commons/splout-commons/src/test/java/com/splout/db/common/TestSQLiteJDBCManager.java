package com.splout.db.common;

/*
 * #%L
 * Splout SQL commons
 * %%
 * Copyright (C) 2012 Datasalt Systems S.L.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;

import com.splout.db.common.JSONSerDe.JSONSerDeException;

public class TestSQLiteJDBCManager {

	public static String TEST_DB_1 = TestSQLiteJDBCManager.class.getName() + ".1.db";
	public static String TEST_DB_2 = TestSQLiteJDBCManager.class.getName() + ".2.db";

	@SuppressWarnings("rawtypes")
	public void basicTest(final ISQLiteManager manager) throws SQLException, JSONSerDeException,
	    InterruptedException {

		manager.query("DROP TABLE IF EXISTS t;", 100);
		manager.query("CREATE TABLE t (a INT, b TEXT);", 100);

		final int nThreads = 10, nOpsPerThread = 100, nWrites = 1000;

		// Insert some foo data
		manager.exec("BEGIN");
		for(int i = 0; i < nWrites; i++) {
			int randInt = (int) (Math.random() * 100000);
			String fooStr = "foo";
			manager.exec("INSERT INTO t (a, b) VALUES (" + randInt + ", \"" + fooStr + "\")");
		}
		manager.exec("COMMIT");

		String json = manager.query("SELECT COUNT(*) FROM t;", 100);
		ArrayList results = JSONSerDe.deSer(json, ArrayList.class);
  	assertEquals(((Map)results.get(0)).get("COUNT(*)"), 1000);

		// Read with some parallel threads
		ExecutorService service = Executors.newFixedThreadPool(nThreads);
		for(int i = 0; i < nThreads; i++) {
			service.execute(new Runnable() {
				@Override
				public void run() {
					try {
						for(int i = 0; i < nOpsPerThread; i++) {
							manager.query("SELECT * FROM t;", 100);
						}
					} catch(Exception e) {
						throw new RuntimeException(e);
					}
				}
			});
		}
		service.shutdown();
		while(!service.isTerminated()) {
			Thread.sleep(100);
		}

		manager.exec("DROP TABLE t;");
	}

	@Test
	public void test() throws Exception {
//		SploutConfiguration.setDevelopmentJavaLibraryPath();
		
		File dbFile = new File(TEST_DB_1);
		if(dbFile.exists()) {
			dbFile.delete();
		}

		final SQLite4JavaManager sqlite4Java = new SQLite4JavaManager(TEST_DB_1, null);
		basicTest(sqlite4Java);
		sqlite4Java.close();
		dbFile.delete();

		final SQLiteJDBCManager jdbcManager = new SQLiteJDBCManager(TEST_DB_1, 1);
		basicTest(jdbcManager);
		jdbcManager.close();
		
		dbFile.delete();
	}

	public void querySizeLimitingTest(final ISQLiteManager manager) throws SQLException,
	    ClassNotFoundException, JSONSerDeException {
		manager.query("DROP TABLE IF EXISTS t;", 100);
		manager.query("CREATE TABLE t (a INT, b TEXT);", 100);

		int nWrites = 100;

		// Insert some foo data
		manager.exec("BEGIN");
		for(int i = 0; i < nWrites; i++) {
			int randInt = (int) (Math.random() * 100000);
			String fooStr = "foo";
			manager.exec("INSERT INTO t (a, b) VALUES (" + randInt + ", \"" + fooStr + "\")");
		}
		manager.exec("COMMIT");

		// Query with hard limit = 100
		@SuppressWarnings("rawtypes")
		ArrayList result = JSONSerDe.deSer(manager.query("SELECT * FROM t;", 100), ArrayList.class);
		assertEquals(100, result.size());

		// Query with hard limit = 10
		result = JSONSerDe.deSer(manager.query("SELECT * FROM t;", 10), ArrayList.class);
		assertEquals(10, result.size());

		// Query with hard limit = 1
		result = JSONSerDe.deSer(manager.query("SELECT * FROM t;", 1), ArrayList.class);
		assertEquals(1, result.size());
	}

	@Test
	public void testQuerySizeLimiting() throws SQLException, ClassNotFoundException, JSONSerDeException {
//		SploutConfiguration.setDevelopmentJavaLibraryPath();

		File dbFile = new File(TEST_DB_2);
		if(dbFile.exists()) {
			dbFile.delete();
		}
		final SQLiteJDBCManager manager = new SQLiteJDBCManager(TEST_DB_2, 1);
		querySizeLimitingTest(manager);
		manager.close();
		dbFile.delete();
		
		final SQLite4JavaManager sqlite4Java = new SQLite4JavaManager(TEST_DB_2, null);
		querySizeLimitingTest(sqlite4Java);
		sqlite4Java.close();
		dbFile.delete();
	}
}
