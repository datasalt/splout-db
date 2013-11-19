package com.splout.db.hadoop.engine;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.splout.db.common.CompressorUtil;
import com.splout.db.common.JSONSerDe;
import com.splout.db.common.engine.EmbeddedMySQL;
import com.splout.db.common.engine.EmbeddedMySQL.EmbeddedMySQLConfig;
import com.splout.db.common.engine.MySQLManager;

@SuppressWarnings("serial")
public class TestMySQLOutputFormat extends SploutSQLOutputFormatTester {

	@SuppressWarnings("rawtypes")
	@Test
	public void test() throws Exception {
		Runtime.getRuntime().exec("rm -rf " + OUTPUT + "-mysql").waitFor();
		
		runTest(MySQLOutputFormat.class);

		// Assert that the DB has been created successfully

		File dbFile = new File(OUTPUT + "/0.db");
		File uncompressTo = new File(OUTPUT + "-mysql");
				
		EmbeddedMySQLConfig config = new EmbeddedMySQLConfig(EmbeddedMySQLConfig.DEFAULT_PORT,
		    EmbeddedMySQLConfig.DEFAULT_USER, EmbeddedMySQLConfig.DEFAULT_PASS, uncompressTo,
		    null);
		EmbeddedMySQL mySQL = new EmbeddedMySQL(config);
		
		// Trick: start mysql first on the empty dir, stop it, uncompress data, start it again
		// This is because mySQL creates some databases by default which doesn't create if "data" already exists
		// So we don't need to add them to the produced zip (1.6 MB less).
		try {
			mySQL.start(true);
			mySQL.stop();
			
			CompressorUtil.uncompress(dbFile, uncompressTo);

			mySQL.start(false);
			MySQLManager manager = new MySQLManager(config, MySQLOutputFormat.GENERATED_DB_NAME, 1);

			List list = JSONSerDe.deSer(manager.query("SELECT * FROM schema1;", 100), ArrayList.class);
			assertEquals(6, list.size());
			list = JSONSerDe.deSer(manager.query("SELECT * FROM schema2;", 100), ArrayList.class);
			assertEquals(2, list.size());

			manager.close();
		} finally {
			mySQL.stop();
		}
	}
}
