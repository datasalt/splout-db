package com.splout.db.common.engine;

/*
 * #%L
 * Splout SQL Server
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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.junit.Test;

import com.google.common.io.Files;
import com.mysql.management.util.QueryUtil;
import com.splout.db.engine.EmbeddedMySQL;
import com.splout.db.engine.MySQLManager;

public class TestEmbeddedMySQL {

	public static void insertData(Connection conn) throws IOException {
		String query = "CREATE TABLE `test_table` (`idpublisher` int(11) DEFAULT NULL, `idsite` int(11) DEFAULT NULL, `country_iso` varchar(4) DEFAULT NULL, `idzone` int(11) DEFAULT NULL, `hits` int(11) DEFAULT NULL, `cpm_value` double DEFAULT NULL, `ddate` varchar(16) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=latin1";
		QueryUtil util = new QueryUtil(conn);
		util.execute(query);

		util.execute("BEGIN");

		for(String insert : Files.readLines(new File("src/test/resources/test.mysql"),
		    Charset.defaultCharset())) {
			String[] fields = insert.split(",");
			String q = "INSERT INTO test_table VALUES (";
			for(int i = 0; i < fields.length; i++) {
				String val = fields[i];
				if(val.length() < 1) {
					val = "NULL";
				} else {
					if(i == 2 || i == 6) {
						val = "'" + val + "'";
					}
				}
				q += val;
				if(i != fields.length - 1) {
					q += ",";
				}
			}
			q += ");";
			util.execute(q);
		}

		util.execute("COMMIT");
	}

	@Test
	public void test() throws ClassNotFoundException, SQLException, IOException, InterruptedException {
		EmbeddedMySQL mysql = new EmbeddedMySQL();
		mysql.start(true);
		Connection conn = null;
		try {
			MySQLManager manager = new MySQLManager(mysql.getConfig(), "test", 10);
			conn = manager.getConnectionFromPool();

			insertData(conn);

			List<?> l = new QueryUtil(conn).executeQuery("SELECT * FROM test_table LIMIT 10;");
			assertEquals(10, l.size());

		} finally {
			if(conn != null) {
				conn.close();
			}
			mysql.stop();
		}
	}
}
