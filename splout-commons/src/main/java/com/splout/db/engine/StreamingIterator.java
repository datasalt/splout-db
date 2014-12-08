package com.splout.db.engine;

public interface StreamingIterator {

  public String getQuery();  
  public void columns(String[] columns);  
  public void collect(Object[] result);  
  public void endStreaming();
}
