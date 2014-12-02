package com.splout.db.common;

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
