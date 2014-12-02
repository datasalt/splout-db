package com.splout.db.engine;

import com.splout.db.common.QueryResult;

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

/**
 * For cursor-aware managers, the result of the query to be returned together
 * with a cursorId
 */
public class ResultAndCursorId {
  public static int NO_CURSOR = -1;

  private final QueryResult result;
  private final int cursorId;

  public ResultAndCursorId(QueryResult result, int cursorId) {
    this.result = result;
    this.cursorId = cursorId;
  }

  public QueryResult getResult() {
    return result;
  }

  public int getCursorId() {
    return cursorId;
  }
}