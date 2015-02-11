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
   * An exception that is thrown when something bad happens and the stream needs to be prematurely finished.
   */
  @SuppressWarnings("serial")
  public static class StreamingTerminationException extends Exception {
 
    public StreamingTerminationException(String msg) {
      super(msg);
    }
    
    public StreamingTerminationException(String msg, Throwable t) {
      super(msg, t);
    }
  }
  
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
  public void collect(Object[] result) throws StreamingTerminationException;
  /**
   * Used to notify the end of the streaming
   */
  public void endStreaming();
}
