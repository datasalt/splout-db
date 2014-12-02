package com.splout.db.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.splout.db.common.JSONSerDe.JSONSerDeException;

/**
 * Object returned by an implementing {@link EngineManager} containing the
 * results of a query or a command.
 */
public class QueryResult {

  private String[] columnNames;
  private List<Object[]> results;

  public QueryResult(String[] columnNames, List<Object[]> results) {
    this.columnNames = columnNames;
    this.results = results;
  }

  public static QueryResult emptyQueryResult() {
    return new QueryResult(new String[0], new ArrayList<Object[]>());
  }

  public String[] getColumnNames() {
    return columnNames;
  }

  public List<Object[]> getResults() {
    return results;
  }

  public List<Map<String, Object>> mapify() {
    List<Map<String, Object>> res = new ArrayList<Map<String, Object>>();
    for (int i = 0; i < results.size(); i++) {
      Map<String, Object> map = new HashMap<String, Object>();
      for (int j = 0; j < columnNames.length; j++) {
        map.put(columnNames[j], results.get(i)[j]);
      }
      res.add(map);
    }
    return res;
  }

  public String jsonize() throws JSONSerDeException {
    return JSONSerDe.ser(mapify());
  }
}
