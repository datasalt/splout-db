package com.splout.db.common.engine;

import org.junit.Test;

public class TestMySQLManager extends SQLManagerTester {

	@Test
	public void test() throws Exception {
		EmbeddedMySQL mysql = new EmbeddedMySQL();
		mysql.start(true);
		try {
			final MySQLManager jdbcManager = new MySQLManager(mysql.getConfig(), "test", 10);
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
			final MySQLManager jdbcManager = new MySQLManager(mysql.getConfig(), "test", 10);
			querySizeLimitingTest(jdbcManager);
		} finally {
			mysql.stop();
		}
	}
}
