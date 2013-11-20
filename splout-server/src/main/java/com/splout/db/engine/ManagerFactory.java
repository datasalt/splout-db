package com.splout.db.engine;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.splout.db.common.CompressorUtil;
import com.splout.db.common.SploutConfiguration;
import com.splout.db.common.TimeoutThread;
import com.splout.db.dnode.DNodeProperties;
import com.splout.db.engine.EmbeddedMySQL.EmbeddedMySQLConfig;
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

	public void init(SploutConfiguration config) {
		timeoutThread = new TimeoutThread(config.getLong(DNodeProperties.MAX_QUERY_TIME));
		timeoutThread.start();
	}

	public void close() {
		timeoutThread.interrupt();
		for(EmbeddedMySQL mySQL : embeddedMySQLs) {
			mySQL.stop();
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

		if(engine.equals(Engine.SQLITE)) {
			manager = new SQLite4JavaManager(dbFolder + "/" + dbFile, partitionMetadata.getInitStatements());
			((SQLite4JavaManager) manager).setTimeoutThread(timeoutThread);

		} else if(engine.equals(Engine.MYSQL)) {
			File mysqlFolder = new File(dbFolder, "mysql");

			int port = EmbeddedMySQL.getNextAvailablePort();
			
			EmbeddedMySQLConfig config = new EmbeddedMySQLConfig(port, EmbeddedMySQLConfig.DEFAULT_USER,
			    EmbeddedMySQLConfig.DEFAULT_PASS, mysqlFolder, null);
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
		} else {
			throw new IllegalArgumentException("Engine not implemented: " + engine);
		}

		return manager;
	}
}
