package com.splout.db.engine;

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
