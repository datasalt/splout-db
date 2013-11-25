package com.splout.db.engine.redis;

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

import redis.clients.jedis.Jedis;
import redis.embedded.RedisServer;

import com.splout.db.common.JSONSerDe;
import com.splout.db.common.JSONSerDe.JSONSerDeException;
import com.splout.db.engine.EngineManager;

public class RedisManager implements EngineManager {

	Jedis jedis;
	RedisServer server;

	public RedisManager(int port) {
		jedis = new Jedis("localhost", port);
	}

	public void saveReferenceTo(RedisServer server) {
		this.server = server;
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
	public void close() throws EngineException {
		jedis.quit();
		if(server != null) {
			server.stop();
		}
	}

	// https://github.com/joeferner/redis-commander
	public static void main(String[] args) throws EngineException, IOException {
		RedisServer redisServer = new RedisServer(new File("/home/pere/redis-2.8.1/src/redis-server"), 6379);
		redisServer.start();

		RedisManager manager = new RedisManager(6379);
		System.out.println(manager.exec("foo"));
		manager.close();

		System.in.read();

		// dir ./
		// dbfilename dump.rdb
		redisServer.stop();
	}
}
