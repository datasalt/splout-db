package com.splout.db.engine;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;

public class BaseTest {

	public static Configuration getBaseConfiguration() {
		Configuration conf = new PropertiesConfiguration();
		if(System.getenv("REDIS_HOME") == null) {
			throw new RuntimeException("REDIS_HOME must be defined to run splout-redis unit tests");
		}
		String redisServerPath = System.getenv("REDIS_HOME") + "/src/redis-server";
		conf.setProperty(RedisManager.REDIS_EXECUTABLE_CONF, redisServerPath);
		return conf;
	}
}
