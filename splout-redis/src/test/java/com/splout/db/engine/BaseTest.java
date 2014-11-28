package com.splout.db.engine;

/*
 * #%L
 * Splout Redis
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

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;

public class BaseTest {

  public static Configuration getBaseConfiguration() {
    Configuration conf = new PropertiesConfiguration();
    if (System.getenv("REDIS_HOME") == null) {
      throw new RuntimeException("REDIS_HOME must be defined to run splout-redis unit tests");
    }
    String redisServerPath = System.getenv("REDIS_HOME") + "/src/redis-server";
    conf.setProperty(RedisManager.REDIS_EXECUTABLE_CONF, redisServerPath);
    return conf;
  }
}
