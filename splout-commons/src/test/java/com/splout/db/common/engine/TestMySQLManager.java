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

import org.junit.Test;

import com.splout.db.engine.EmbeddedMySQL;
import com.splout.db.engine.MySQLManager;

public class TestMySQLManager extends SQLManagerTester {

	@Test
	public void test() throws Exception {
		EmbeddedMySQL mysql = new EmbeddedMySQL();
		mysql.start(true);
		try {
			// this only works with a pool of 1 connection, because the tester issues BEGIN / COMMIT with query() engine method
			final MySQLManager jdbcManager = new MySQLManager(mysql.getConfig(), "test", 1);
			basicTest(jdbcManager);
			jdbcManager.close();

		} finally {
			mysql.stop();
		}
	}

	@Test
	public void testQuerySizeLimiting() throws Exception {
		EmbeddedMySQL mysql = new EmbeddedMySQL();
		mysql.start(true);
		try {
			// this only works with a pool of 1 connection, because the tester issues BEGIN / COMMIT with query() engine method
			final MySQLManager jdbcManager = new MySQLManager(mysql.getConfig(), "test", 1);
			querySizeLimitingTest(jdbcManager);
		} finally {
			mysql.stop();
		}
	}
}
