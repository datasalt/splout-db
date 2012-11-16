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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;
import com.splout.db.common.JSONSerDe.JSONSerDeException;

/**
 * SQL Wrapper for querying SQLite through a connection pool using BoneCP (<a
 * href="http://jolbox.com">http://jolbox.com/</a>).
 */
public class SQLiteJDBCManager implements ISQLiteManager {

	private final static Log log = LogFactory.getLog(SQLiteJDBCManager.class);
	BoneCP connectionPool = null;

	public SQLiteJDBCManager(String dbFile, int nConnections) throws SQLException, ClassNotFoundException {
		// Load the sqlite-JDBC driver using the current class loader
		Class.forName("org.sqlite.JDBC");
		BoneCPConfig config = new BoneCPConfig();
		config.setJdbcUrl("jdbc:sqlite:" + dbFile);
		config.setMinConnectionsPerPartition(nConnections);
		config.setMaxConnectionsPerPartition(nConnections);
		config.setUsername("foo");
		config.setPassword("foo");
		config.setPartitionCount(1);

		connectionPool = new BoneCP(config); // setup the connection pool
	}

	public Connection getConnectionFromPool() throws SQLException {
		return connectionPool.getConnection();
	}

	/**
	 * The contract of this function is to return a JSON-ized ArrayList of JSON Objects which in Java are represented as
	 * Map<String, Object>. So, for a query with no results, an empty ArrayList is returned.
	 */
	public String query(String query, int maxResults) throws SQLException, JSONSerDeException {
		long start = System.currentTimeMillis();
		Connection connection = connectionPool.getConnection(); // fetch a connection
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = connection.createStatement();
			String result = null;
			if(stmt.execute(query)) {
				rs = stmt.getResultSet();
				result = JSONSerDe.ser(convertResultSetToList(rs, maxResults));
			} else {
				result = JSONSerDe.ser(new ArrayList<HashMap<String, Object>>());
			}
			long end = System.currentTimeMillis();
			log.info(Thread.currentThread().getName() + ": Query [" + query + "] handled in [" + (end - start)
			    + "] ms.");
			return result;
		} finally {
			if(rs != null) {
				rs.close();
			}
			if(stmt != null) {
				stmt.close();
			}
			connection.close();
		}
	}

	// -------- //

	private static List<HashMap<String, Object>> convertResultSetToList(ResultSet rs, int maxResults)
	    throws SQLException {
		ResultSetMetaData md = rs.getMetaData();
		int columns = md.getColumnCount();
		List<HashMap<String, Object>> list = new ArrayList<HashMap<String, Object>>();
		while(rs.next() && list.size() < maxResults) {
			HashMap<String, Object> row = new HashMap<String, Object>(columns);
			for(int i = 1; i <= columns; ++i) {
				row.put(md.getColumnName(i), rs.getObject(i));
			}
			list.add(row);
		}
		return list;
	}

	public void close() {
		connectionPool.shutdown();
	}

	@Override
  public String exec(String query) throws SQLException, JSONSerDeException {
	  return query(query, 1);
  }
}
