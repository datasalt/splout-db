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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;

/**
 * A Thread that is in charge of killing long-running queries.
 */
public class TimeoutThread extends Thread {

	private final static Log log = LogFactory.getLog(TimeoutThread.class);

	private ConcurrentHashMap<SQLiteConnection, QueryAndTime> currentQueries = new ConcurrentHashMap<SQLiteConnection, QueryAndTime>();
	private long timeout;

	static class QueryAndTime {
		String query;
		Long time;
		
		public QueryAndTime(String query, Long time) {
			this.query = query;
			this.time = time;
		}
	}
	
	/**
	 * @param timeout
	 *          The timeout in milliseconds. If a SQLite connection monitored by this Thread has a query that runs for
	 *          more than this, it will be interrupted and the query will return an error.
	 */
	public TimeoutThread(long timeout) {
		this.timeout = timeout;
	}

	@Override
	public void run() {
		try {
			while(true) {
				Thread.sleep(1000);
				long now = System.currentTimeMillis();
				for(Map.Entry<SQLiteConnection, QueryAndTime> entry: currentQueries.entrySet()) {
					if((now - entry.getValue().time) > timeout) {
						// Timeout: we should interrupt this connection!
						SQLiteConnection conn = entry.getKey();
						try {
							
							/*
							 * Even though SQLiteConnections are not thread-safe, this method *IS* thread-safe
							 * and that's why we can implement this thread. The thread that launched the query
							 * is busy waiting for the result so another thread must interrupt it!
							 * 
							 * SQLite4Java docs: http://almworks.com/sqlite4java/javadoc/com/almworks/sqlite4java/SQLiteConnection.html#interrupt()
							 * SQLite docs: http://www.sqlite.org/c3ref/interrupt.html
							 */
							log.info("Long running query [" + entry.getValue().query + "] ran for more than [" + timeout + "] ms. Interrupting it!");
	            conn.interrupt();
            } catch(SQLiteException e) {
            	//
            }
					}
				}
			}
		} catch(InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * A Thread provides its thread-local connection to be monitored when a query starts.
	 * The SQL query is provided just for logging purposes.
	 */
	public void startQuery(SQLiteConnection connection, String query) {
		currentQueries.put(connection, new QueryAndTime(query, System.currentTimeMillis()));
	}

	/**
	 * The same Thread that provided this connection in startQuery() removes it from the monitoring list. So it is
	 * important to understand that this class makes the assumption that each SQLite Thread will use only one connection
	 * (Thread-local). {@link SQLite4JavaManager} behaves like this.
	 */
	public void endQuery(SQLiteConnection connection) {
		currentQueries.remove(connection);
	}
}
