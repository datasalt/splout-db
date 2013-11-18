package com.splout.db.common.engine;

import org.junit.Test;

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
