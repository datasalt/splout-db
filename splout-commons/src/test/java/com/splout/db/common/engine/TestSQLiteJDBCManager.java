package com.splout.db.common.engine;

/*
 * #%L
 * Splout SQL commons
 * %%
 * Copyright (C) 2012 - 2013 Datasalt Systems S.L.
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

import java.io.File;

import org.junit.Test;

public class TestSQLiteJDBCManager extends SQLManagerTester {

	public static String TEST_DB_1 = TestSQLiteJDBCManager.class.getName() + ".1.db";
	public static String TEST_DB_2 = TestSQLiteJDBCManager.class.getName() + ".2.db";

	@Test
	public void test() throws Exception {
		File dbFile = new File(TEST_DB_1);
		if(dbFile.exists()) {
			dbFile.delete();
		}

		final SQLiteJDBCManager jdbcManager = new SQLiteJDBCManager(TEST_DB_1, 1);
		basicTest(jdbcManager);
		jdbcManager.close();

		dbFile.delete();
	}
	
	@Test
	public void testQuerySizeLimiting() throws Exception {
		File dbFile = new File(TEST_DB_2);
		if(dbFile.exists()) {
			dbFile.delete();
		}

		final SQLiteJDBCManager jdbcManager = new SQLiteJDBCManager(TEST_DB_2, 1);
		querySizeLimitingTest(jdbcManager);
		jdbcManager.close();
		dbFile.delete();
	}
}
