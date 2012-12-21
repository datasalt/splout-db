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

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.splout.db.common.JSONSerDe.JSONSerDeException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SQL Wrapper for querying SQLite by using sqlite4java (http://code.google.com/p/sqlite4java).
 */
public class SQLite4JavaManager implements ISQLiteManager {

	private final static Log log = LogFactory.getLog(SQLite4JavaManager.class);
	
	private final File dbFile;
	private final List<String> initStatements;
	
	// If present, will monitor long-running queries and kill them if needed
	private TimeoutThread timeoutThread = null;
	
	ThreadLocal<SQLiteConnection> db = new ThreadLocal<SQLiteConnection>() {

		protected SQLiteConnection initialValue() {
			log.info(Thread.currentThread().getName() + " requests a new connection to " + dbFile);
			SQLiteConnection conn = new SQLiteConnection(dbFile);
			try {
				conn.open(true);
        // Executing some defaults
        conn.exec("PRAGMA cache_size=20");
        // User provided initStatements
				if(initStatements != null) {
					for(String initStatement: initStatements) {
						conn.exec(initStatement);
					}
				}
			} catch(SQLiteException e) {
				e.printStackTrace();
				return null;
			}
      log.info("New SQLite connection open with " + dbFile);
			return conn;
		}
	};

	public SQLite4JavaManager(String dbFile, List<String> initStatements) throws SQLException {
		this.dbFile = new File(dbFile);
		this.initStatements = initStatements;
	}

	/**
	 * Optionally sets a {@link TimeoutThread} that will take care of cancelling long-running queries.
	 * If present, each SQLiteConnectiona associated with each thread will be monitored by this thread
	 * to see if there is some query that needs to be interrupted.
	 */
	public void setTimeoutThread(TimeoutThread timeoutThread) {
		this.timeoutThread = timeoutThread;
	}
	
	@Override
	public String exec(String query) throws SQLException, JSONSerDeException {
		try {
			db.get().exec(query);
			return "[{ \"status\": \"OK\" }]";
		} catch(SQLiteException e) {
			throw new SQLException(e);
		}
	}

	@Override
	public String query(String query, int maxResults) throws SQLException, JSONSerDeException {
		SQLiteStatement st = null;
		try {
			SQLiteConnection conn = db.get();
			if(timeoutThread != null) {
				timeoutThread.startQuery(conn, query);
			}
			// We don't want to cache the statements here so we use "false"
			// Don't use the method without boolean because it will use cached = true!!!
			st = conn.prepare(query, false);
			if(timeoutThread != null) {
				timeoutThread.endQuery(conn);
			}
			List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
			if(st.step()) {
				do {
					// true if there is data (SQLITE_ROW) was returned, false if statement has been completed (SQLITE_DONE)
					Map<String, Object> objectToRead = new HashMap<String, Object>();
					for(int i = 0; i < st.columnCount(); i++) {
						objectToRead.put(st.getColumnName(i), st.columnValue(i));
					}
					list.add(objectToRead);
				} while(st.step() && list.size() < maxResults);
			}
			if(list.size() == maxResults) {
				throw new SQLException("Hard limit on number of results reached (" + maxResults + "), please use a LIMIT for this query.");
			}
			return JSONSerDe.ser(list);
		} catch(SQLiteException e) {
			throw new SQLException(e);
		} finally {
			if(st != null) {
				st.dispose();
			}
		}
	}

	@Override
	public void close() {
		db.get().dispose();
	}

	@Override
	public Connection getConnectionFromPool() throws SQLException {
		throw new RuntimeException("Not implemented");
	}
}
