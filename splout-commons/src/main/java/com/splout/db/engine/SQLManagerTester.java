package com.splout.db.engine;

/*
 * #%L
 * Splout SQL commons
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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import com.splout.db.common.JSONSerDe;
import com.splout.db.common.JSONSerDe.JSONSerDeException;
import com.splout.db.engine.EngineManager.EngineException;

/**
 * Generic tester logic for any EngineManager that implements SQL.
 */
public class SQLManagerTester {

	@SuppressWarnings("rawtypes")
	public void basicTest(final EngineManager manager) throws
	    InterruptedException, EngineException, JSONSerDeException {

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
		assertEquals(((Map) results.get(0)).get("COUNT(*)"), nWrites);

		final AtomicBoolean failed = new AtomicBoolean(false);
		
		// Read with some parallel threads
		ExecutorService service = Executors.newFixedThreadPool(nThreads);
		for(int i = 0; i < nThreads; i++) {
			service.execute(new Runnable() {
				@Override
				public void run() {
					try {
						for(int i = 0; i < nOpsPerThread; i++) {
							manager.query("SELECT * FROM t;", 2000);
						}
					} catch(Exception e) {
						failed.set(true);
					}
				}
			});
		}
		service.shutdown();
		while(!service.isTerminated()) {
			Thread.sleep(100);
		}

		assertEquals(false, failed.get());
		manager.exec("DROP TABLE t;");
	}

	public void querySizeLimitingTest(final EngineManager manager) throws SQLException,
	    ClassNotFoundException, JSONSerDeException, EngineException {
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
		try {
			JSONSerDe.deSer(manager.query("SELECT * FROM t;", 100), ArrayList.class);
			throw new AssertionError("Exception was not thrown but it was expected (query hard limit)");
		} catch(EngineException e) {
		}

		// Query with hard limit = 10
		try {
			JSONSerDe.deSer(manager.query("SELECT * FROM t;", 10), ArrayList.class);
			throw new AssertionError("Exception was not thrown but it was expected (query hard limit)");
		} catch(EngineException e) {
		}

		// Query with hard limit = 1
		try {
			JSONSerDe.deSer(manager.query("SELECT * FROM t;", 1), ArrayList.class);
			throw new AssertionError("Exception was not thrown but it was expected (query hard limit)");
		} catch(EngineException e) {
		}
	}
}
