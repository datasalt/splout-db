package com.splout.db.engine;

/*
 * #%L
 * Splout SQL commons
 * %%
 * Copyright (C) 2012 - 2014 Datasalt Systems S.L.
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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheException;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import net.sf.ehcache.event.CacheEventListenerAdapter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.splout.db.common.QueryResult;
import com.splout.db.common.TimeoutThread;

public class SQLite4JavaClient {

  private final static Log log = LogFactory.getLog(SQLite4JavaClient.class);

  private File dbFile;
  private List<String> initStatements;

  // If present, will monitor long-running queries and kill them if needed
  private TimeoutThread timeoutThread = null;

  public static class ThreadAndConnection {

    String threadName;
    SQLiteConnection conn;
  }

  // Will save all opened connections from different Threads so we can close
  // them all afterwards
  private CopyOnWriteArrayList<ThreadAndConnection> allOpenedConnections = new CopyOnWriteArrayList<ThreadAndConnection>();
  // Expired connections go to this central/static trash so each thread can
  // check if it has something to close...
  // As SQLiteConnections can only be used and closed by the SAME THREAD that
  // created them, this is the only feasible solution I came up with.
  public static ConcurrentHashMap<String, Set<SQLiteConnection>> CLEAN_UP_AFTER_YOURSELF = new ConcurrentHashMap<String, Set<SQLiteConnection>>();

  private AtomicInteger cursorIdGenerator = new AtomicInteger(0);
  private Cache serverSideCursors;
  private boolean enableCursors;

  ThreadLocal<SQLiteConnection> db = new ThreadLocal<SQLiteConnection>() {

    protected SQLiteConnection initialValue() {
      log.info(Thread.currentThread().getName() + " requests a new connection to " + dbFile);
      SQLiteConnection conn = new SQLiteConnection(dbFile);
      try {
        conn.open(true);
        conn.setExtensionLoadingEnabled(true); // TODO Make optional
        // Executing some defaults
        conn.exec("PRAGMA cache_size=20");
        // User provided initStatements
        if (initStatements != null) {
          for (String initStatement : initStatements) {
            conn.exec(initStatement);
          }
        }
      } catch (SQLiteException e) {
        if (conn != null && conn.isOpen()) {
          conn.dispose();
        }
        e.printStackTrace();
        return null;
      }
      log.info("New SQLite connection open with " + dbFile);
      ThreadAndConnection tConn = new ThreadAndConnection();
      tConn.threadName = Thread.currentThread().getName();
      tConn.conn = conn;
      if (CLEAN_UP_AFTER_YOURSELF.get(tConn.threadName) == null) {
        // Initialize the connection trash so we can always check it without NPE
        // risks
        CLEAN_UP_AFTER_YOURSELF.put(tConn.threadName, new HashSet<SQLiteConnection>());
      }
      allOpenedConnections.add(tConn);
      return conn;
    }
  };

  public SQLite4JavaClient(String dbFile, List<String> initStatements, boolean enableCursors, int serverSideCursorsTimeout) {
    this.dbFile = new File(dbFile);
    this.initStatements = initStatements;
    this.enableCursors = enableCursors;
    if (enableCursors) {
      serverSideCursors = new Cache("serverSideCursors", 0, false, false, Integer.MAX_VALUE, serverSideCursorsTimeout);
      serverSideCursors.initialise();
      serverSideCursors.getCacheEventNotificationService().registerListener(new CacheEventListenerAdapter() {
        @Override
        public void notifyElementEvicted(Ehcache cache, Element element) {
          ((SQLiteStatement) element.getObjectValue()).dispose();
        }

        @Override
        public void notifyElementRemoved(Ehcache cache, Element element) throws CacheException {
          ((SQLiteStatement) element.getObjectValue()).dispose();
        }

        @Override
        public void notifyElementExpired(Ehcache cache, Element element) {
          ((SQLiteStatement) element.getObjectValue()).dispose();
        }
      });
    }
  }

  /**
   * Optionally sets a {@link TimeoutThread} that will take care of cancelling
   * long-running queries. If present, each SQLiteConnectiona associated with
   * each thread will be monitored by this thread to see if there is some query
   * that needs to be interrupted.
   */
  public void setTimeoutThread(TimeoutThread timeoutThread) {
    this.timeoutThread = timeoutThread;
  }

  public QueryResult exec(String query) throws SQLException {
    try {
      db.get().exec(query);
      List<Object[]> resultList = new ArrayList<Object[]>();
      String[] columnNames = new String[] { "status" };
      resultList.add(new Object[] { "OK" });
      return new QueryResult(columnNames, resultList);
    } catch (SQLiteException e) {
      throw new SQLException(e);
    }
  }

  /**
   * Non-cursor-aware method
   */
  public QueryResult query(String query, int maxResults) throws SQLException {
    return query(query, ResultAndCursorId.NO_CURSOR, maxResults, true).getResult();
  }

  /**
   * For unit-testing
   */
  Cache getServerSideCursors() {
    return serverSideCursors;
  }

  /**
   * Cursor-aware method
   */
  public ResultAndCursorId query(String query, int previousCursorId, int maxResults, boolean throwExceptionIfHardLimit) throws SQLException {

    String t = Thread.currentThread().getName();
    Set<SQLiteConnection> pendingClose = CLEAN_UP_AFTER_YOURSELF.get(t);
    // Because SQLiteConnection can only be closed by owner Thread, here we need
    // to check if we
    // have some pending connections to close...
    if (pendingClose != null && pendingClose.size() > 0) {
      synchronized (pendingClose) {
        Iterator<SQLiteConnection> it = pendingClose.iterator();
        while (it.hasNext()) {
          SQLiteConnection conn = it.next();
          log.info("-- Closed a connection pending diposal: " + conn.getDatabaseFile());
          conn.dispose();
          it.remove();
        }
      }
    }

    if (!enableCursors) {
      previousCursorId = ResultAndCursorId.NO_CURSOR;
    }

    SQLiteStatement st = null;
    int cursorId = ResultAndCursorId.NO_CURSOR;

    try {
      if (previousCursorId != ResultAndCursorId.NO_CURSOR) {
        Element el = serverSideCursors.get(previousCursorId);
        if (el == null) {
          throw new SQLException("Cursor " + previousCursorId + " expired.");
        }
        st = (SQLiteStatement) el.getObjectValue();
      }

      if (st == null) {
        SQLiteConnection conn = db.get();
        if (timeoutThread != null) {
          timeoutThread.startQuery(conn, query);
        }
        // We don't want to cache the statements here so we use "false"
        // Don't use the method without boolean because it will use cached =
        // true!!!
        st = conn.prepare(query, false);
        if (timeoutThread != null) {
          timeoutThread.endQuery(conn);
        }
      }

      List<Object[]> resultList = new ArrayList<Object[]>();
      String[] columnNames = new String[0];

      do {
        st.step();
        if (st.hasRow()) {
          if (columnNames.length == 0) {
            columnNames = new String[st.columnCount()];
            for (int i = 0; i < st.columnCount(); i++) {
              columnNames[i] = st.getColumnName(i);
            }
          }
          // true if there is data (SQLITE_ROW) was returned, false if statement
          // has been completed (SQLITE_DONE)
          Object[] objectToRead = new Object[st.columnCount()];
          for (int i = 0; i < st.columnCount(); i++) {
            objectToRead[i] = st.columnValue(i);
          }
          resultList.add(objectToRead);
        } else {
          break;
        }
      } while (resultList.size() < maxResults);
      if (resultList.size() == maxResults) {
        if (!enableCursors || throwExceptionIfHardLimit) {
          throw new SQLException("Hard limit on number of results reached (" + maxResults
              + "), please enable server side cursors or use a LIMIT for this query.");
        } else {
          cursorId = previousCursorId != ResultAndCursorId.NO_CURSOR ? previousCursorId : cursorIdGenerator.incrementAndGet();
          serverSideCursors.put(new Element(cursorId, st));
        }
      } else {
        if (previousCursorId != ResultAndCursorId.NO_CURSOR) {
          serverSideCursors.remove(previousCursorId);
        }
      }
      return new ResultAndCursorId(new QueryResult(columnNames, resultList), cursorId);
    } catch (SQLiteException e) {
      throw new SQLException(e);
    } finally {
      if (st != null && cursorId == ResultAndCursorId.NO_CURSOR) {
        st.dispose();
      }
    }
  }

  public void close() {
    String thisThread = Thread.currentThread().getName();
    for (ThreadAndConnection tConn : allOpenedConnections) {
      if (thisThread.equals(tConn.threadName)) {
        log.info("-- Closing my own connection to: " + tConn.conn.getDatabaseFile());
        tConn.conn.dispose();
      } else { // needs to be closed at some point by the owner
        Set<SQLiteConnection> set = CLEAN_UP_AFTER_YOURSELF.get(tConn.threadName);
        synchronized (set) { // just in case the owner of the set is iterating
                             // over it
          CLEAN_UP_AFTER_YOURSELF.get(tConn.threadName).add(tConn.conn);
        }
      }
    }
//		timeoutThread.interrupt();
  }
}
