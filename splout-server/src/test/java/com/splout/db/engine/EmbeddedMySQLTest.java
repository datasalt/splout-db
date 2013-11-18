package com.splout.db.engine;

/*
 * #%L
 * Splout SQL Server
 * %%
 * Copyright (C) 2012 - 2013 Datasalt Systems S.L.
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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import org.junit.Test;

import com.google.common.io.Files;
import com.mysql.management.util.QueryUtil;
import com.splout.db.engine.EmbeddedMySQL.EmbeddedMySQLConfig;

public class EmbeddedMySQLTest {

	public static final String DRIVER = "com.mysql.jdbc.Driver";

	public static void insertData(Connection conn) throws IOException {
		String query = "CREATE TABLE IF NOT EXISTS `test_table` (`idpublisher` int(11) DEFAULT NULL, `idsite` int(11) DEFAULT NULL, `country_iso` varchar(4) DEFAULT NULL, `idzone` int(11) DEFAULT NULL, `hits` int(11) DEFAULT NULL, `cpm_value` double DEFAULT NULL, `ddate` varchar(16) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=latin1";
		QueryUtil util = new QueryUtil(conn);
		util.execute(query);

		util.execute("DELETE FROM test_table;");
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
	public void test() throws ClassNotFoundException, SQLException, IOException {
		EmbeddedMySQL mysql = new EmbeddedMySQL();
		mysql.start();
		Connection conn = null;
		try {

			Class.forName(DRIVER);

			String dbName = "test";
			String url = mysql.getLocalConnection(dbName);
			conn = DriverManager.getConnection(url, EmbeddedMySQLConfig.DEFAULT_USER,
			    EmbeddedMySQLConfig.DEFAULT_PASS);

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
