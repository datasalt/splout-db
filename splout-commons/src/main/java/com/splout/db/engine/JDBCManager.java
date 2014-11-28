package com.splout.db.engine;

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

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;
import com.splout.db.common.JSONSerDe;
import com.splout.db.common.JSONSerDe.JSONSerDeException;
import com.splout.db.engine.EngineManager.EngineException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Generic JDBC Manager which can be reused by any engine which is JDBC-compliant.
 */
public class JDBCManager {

  private final static Log log = LogFactory.getLog(JDBCManager.class);
  BoneCP connectionPool = null;

  public JDBCManager(String driver, String connectionUri, int nConnectionsPool, String userName, String password) throws SQLException, ClassNotFoundException {

    Class.forName(driver);

    BoneCPConfig config = new BoneCPConfig();
    config.setJdbcUrl(connectionUri);
    config.setMinConnectionsPerPartition(nConnectionsPool);
    config.setMaxConnectionsPerPartition(nConnectionsPool);
    config.setUsername(userName);
    config.setPassword(password);
    config.setPartitionCount(1);
    config.setDefaultAutoCommit(false);

    connectionPool = new BoneCP(config); // setup the connection pool
  }

  /**
   * The contract of this function is to return a JSON-ized ArrayList of JSON Objects which in Java are represented as
   * Map<String, Object>. So, for a query with no results, an empty ArrayList is returned.
   */
  public String query(String query, int maxResults) throws EngineException {
    long start = System.currentTimeMillis();
    Connection connection = null;
    ResultSet rs = null;
    Statement stmt = null;
    try {
      connection = connectionPool.getConnection(); // fetch a connection
      stmt = connection.createStatement();
      String result = null;
      if (stmt.execute(query)) {
        rs = stmt.getResultSet();
        result = JSONSerDe.ser(convertResultSetToList(rs, maxResults));
      } else {
        result = JSONSerDe.ser(new ArrayList<HashMap<String, Object>>());
      }
      long end = System.currentTimeMillis();
      log.info(Thread.currentThread().getName() + ": Query [" + query + "] handled in [" + (end - start)
          + "] ms.");
      return result;
    } catch (SQLException e) {
      throw new EngineException(e);
    } catch (JSONSerDeException e) {
      throw new EngineException(e);
    } finally {
      try {
        if (rs != null) {
          rs.close();
        }
        if (stmt != null) {
          stmt.close();
        }
        connection.close();
      } catch (SQLException e) {
        throw new EngineException(e);
      }
    }
  }

  // -------- //

  public static List<HashMap<String, Object>> convertResultSetToList(ResultSet rs, int maxResults)
      throws SQLException {
    ResultSetMetaData md = rs.getMetaData();
    int columns = md.getColumnCount();
    List<HashMap<String, Object>> list = new ArrayList<HashMap<String, Object>>();
    while (rs.next() && list.size() < maxResults) {
      HashMap<String, Object> row = new HashMap<String, Object>(columns);
      for (int i = 1; i <= columns; ++i) {
        row.put(md.getColumnName(i), rs.getObject(i));
      }
      list.add(row);
    }
    if (list.size() == maxResults) {
      throw new SQLException("Hard limit on number of results reached (" + maxResults
          + "), please use a LIMIT for this query.");
    }
    return list;
  }

  public void close() {
    connectionPool.shutdown();
  }

  public String exec(String query) throws EngineException {
    return query(query, 1);
  }

  public Connection getConnectionFromPool() throws SQLException {
    return connectionPool.getConnection();
  }
}
