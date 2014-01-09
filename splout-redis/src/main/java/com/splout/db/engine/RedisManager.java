package com.splout.db.engine;

/*
 * #%L
 * Splout SQL commons
 * %%
 * Copyright (C) 2012 - 2013 Datasalt Systems S.L.
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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;

import redis.clients.jedis.Jedis;
import redis.embedded.RedisServer;

import com.splout.db.common.JSONSerDe;
import com.splout.db.common.JSONSerDe.JSONSerDeException;
import com.splout.db.common.PortUtils;
import com.splout.db.common.PortUtils.PortLock;

public class RedisManager implements EngineManager {

	Jedis jedis;
	private RedisServer redisServer = null;
	
	public static String REDIS_EXECUTABLE_CONF = "com.splout.db.engine.RedisManager.redis.executable";
	public static String BASE_PORT_CONF = "com.splout.db.engine.RedisManager.redis.base.port"; 
	final static int DEFAULT_BASE_PORT = 6379; // TODO make parametric
	
	public RedisManager() {
	}

	RedisManager(int port) {
		jedis = new Jedis("localhost",port);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public String exec(String query) throws EngineException {
		try {
			List<HashMap<String, Object>> result = new ArrayList<HashMap<String, Object>>();
			String res = jedis.get(query);
			if(res == null) {
				return JSONSerDe.ser(result);
			}
			result.add(JSONSerDe.deSer(jedis.get(query), HashMap.class));
			return JSONSerDe.ser(result);
		} catch(JSONSerDeException e) {
			throw new EngineException(e);
		}
	}

	@Override
	public String query(String query, int maxResults) throws EngineException {
		return exec(query);
	}

	@Override
	public void close() {
		jedis.quit();
		if(redisServer != null) {
			redisServer.stop();
		}
	}

	@Override
	public void init(File dbFile, Configuration config, List<String> initStatements)
	    throws EngineException {

		File dbFolder = dbFile.getParentFile();

		String redisExecutable = config.getString(REDIS_EXECUTABLE_CONF, null);
		if(redisExecutable == null) {
			throw new EngineException("A Redis executable path should be specified in configuration '" + REDIS_EXECUTABLE_CONF + "'", null);
		}
		
		if(!new File(redisExecutable).exists()) {
			throw new EngineException("The specified Redis executable doesn't exist: " + redisExecutable, null);
		}
		
		int basePort = config.getInt(BASE_PORT_CONF, DEFAULT_BASE_PORT);
		
		File thisServer = new File(dbFolder, "redis-server");
		File thisDataFile = new File(dbFolder, "dump.rdb");
		File actualDataFile = dbFile;
		try {
			Runtime
			    .getRuntime()
			    .exec(
			        new String[] { "ln", "-s", actualDataFile.getAbsolutePath(),
			            thisDataFile.getAbsolutePath() }).waitFor();
			FileUtils.copyFile(new File(redisExecutable), thisServer);
			thisServer.setExecutable(true);

			PortLock portLock = PortUtils.getNextAvailablePort(basePort);
			try {
				redisServer = new RedisServer(thisServer, portLock.getPort());
				redisServer.start();
				jedis = new Jedis("localhost", portLock.getPort());
			} finally {
				portLock.release();
			}
		} catch(InterruptedException e) {
			throw new EngineException(e);
		} catch(IOException e) {
			throw new EngineException(e);
		}
	}
}
