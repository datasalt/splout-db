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

public interface StreamingIterator {

  /**
   * Will contain the query to be executed by the engine
   */
  public String getQuery();  
  /**
   * Can be used by the engine to report the returned column names
   */
  public void columns(String[] columns);  
  /**
   * Can be used by the engine to report every fetched record
   */
  public void collect(Object[] result);
  /**
   * Used to notify the end of the streaming
   */
  public void endStreaming();
}
