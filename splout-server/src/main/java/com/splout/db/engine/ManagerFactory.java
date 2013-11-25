package com.splout.db.engine;

/*
 * #%L
 * Splout SQL Server
 * %%
 * Copyright (C) 2012 - 2013 Datasalt Systems S.L.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;

import redis.embedded.RedisServer;

import com.splout.db.common.CompressorUtil;
import com.splout.db.common.SploutConfiguration;
import com.splout.db.common.TimeoutThread;
import com.splout.db.dnode.DNodeProperties;
import com.splout.db.engine.EmbeddedMySQL.EmbeddedMySQLConfig;
import com.splout.db.engine.PortUtils.PortLock;
import com.splout.db.engine.redis.RedisManager;
import com.splout.db.thrift.PartitionMetadata;

/**
 * Stateful factory where engine-specific business logic for instantiating a {@link EngineManager} for each
 * {@link Engine} is written. The DNode calls this factory at startup, closing time and everytime it needs to open a new
 * manager for a new data partition with associated {@link PartitionMetadata}.
 */
public class ManagerFactory {

	// private final static Log log = LogFactory.getLog(ManagerFactory.class);

	// This thread will interrupt long-running queries (only available for SQLite)
	private TimeoutThread timeoutThread;
	private List<EmbeddedMySQL> embeddedMySQLs = new ArrayList<EmbeddedMySQL>();

	private List<RedisServer> redisServers = new ArrayList<RedisServer>();
	private File redisExecutable = new File("/home/pere/redis-2.8.1/src/redis-server"); // TODO Read from splout conf.

	public void init(SploutConfiguration config) {
		timeoutThread = new TimeoutThread(config.getLong(DNodeProperties.MAX_QUERY_TIME));
		timeoutThread.start();
	}

	public void close() {
		timeoutThread.interrupt();
		for(EmbeddedMySQL mySQL : embeddedMySQLs) {
			mySQL.stop();
		}
		for(RedisServer redis : redisServers) {
			redis.stop();
		}
	}

	public EngineManager getManagerIn(File dbFolder, PartitionMetadata partitionMetadata) throws Exception {
		EngineManager manager = null;

		Engine engine = Engine.getDefault();
		// Assume default engine in case of no engine (to preserve backwards compatibility)
		if(partitionMetadata.getEngineId() != null) {
			engine = Engine.valueOf(partitionMetadata.getEngineId());
		}

		// Currently using first ".db" file found (there should be only one!)
		String dbFile = null;
		for(String file : dbFolder.list()) {
			if(file.endsWith(".db")) {
				dbFile = file;
				break;
			}
		}
		if(dbFile == null) {
			throw new RuntimeException("Can't find .db file in directory: " + dbFolder);
		}

		/*
		 * SQLite
		 */
		if(engine.equals(Engine.SQLITE)) {
			manager = new SQLite4JavaManager(dbFolder + "/" + dbFile, partitionMetadata.getInitStatements());
			((SQLite4JavaManager) manager).setTimeoutThread(timeoutThread);

			/*
			 * MySQL
			 */
		} else if(engine.equals(Engine.MYSQL)) {
			File mysqlFolder = new File(dbFolder, "mysql");

			PortLock portLock = PortUtils.getNextAvailablePort(EmbeddedMySQLConfig.DEFAULT_PORT);
			try {
				EmbeddedMySQLConfig config = new EmbeddedMySQLConfig(portLock.getPort(),
				    EmbeddedMySQLConfig.DEFAULT_USER, EmbeddedMySQLConfig.DEFAULT_PASS, mysqlFolder, null);
				EmbeddedMySQL mySQL = new EmbeddedMySQL(config);
				embeddedMySQLs.add(mySQL);

				// Trick: start mysql first on the empty dir, stop it, uncompress data, start it again
				// This is because mySQL creates some databases by default which doesn't create if "data" already exists
				// So we don't need to add them to the produced zip (1.6 MB less).
				mySQL.start(true);
				mySQL.stop();

				CompressorUtil.uncompress(new File(dbFolder, dbFile), mysqlFolder);

				mySQL.start(false);
				manager = new MySQLManager(config, "splout", 1);
				((MySQLManager) manager).saveReferenceTo(mySQL); // so it can be lazily closed by EHCache
			} finally {
				portLock.release();
			}
			/*
			 * Redis
			 */
		} else if(engine.equals(Engine.REDIS)) {
			File thisServer = new File(dbFolder, "redis-server");
			File thisDataFile = new File(dbFolder, "dump.rdb");
			File actualDataFile = new File(dbFolder, dbFile);
			Runtime
			    .getRuntime()
			    .exec(
			        new String[] { "ln", "-s", 
			            actualDataFile.getAbsolutePath(),
			            thisDataFile.getAbsolutePath() }).waitFor();
			FileUtils.copyFile(redisExecutable, thisServer);
			thisServer.setExecutable(true);

			PortLock portLock = PortUtils.getNextAvailablePort(6379);
			try {
				RedisServer redisServer = new RedisServer(thisServer, portLock.getPort());
				redisServer.start();
				redisServers.add(redisServer);
				manager = new RedisManager(portLock.getPort());
			} finally {
				portLock.release();
			}
		} else {
			throw new IllegalArgumentException("Engine not implemented: " + engine);
		}

		return manager;
	}
}
