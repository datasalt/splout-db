package com.splout.db.engine;

public class RedisEngine extends SploutEngine {

	@Override
  public String getId() {
	  return "REDIS";
  }

	@Override
  public String getOutputFormatClass() {
	  return RedisOutputFormat.class.getName();
  }

	@Override
  public String getEngineManagerClass() {
	  return RedisManager.class.getName();
  }
}
