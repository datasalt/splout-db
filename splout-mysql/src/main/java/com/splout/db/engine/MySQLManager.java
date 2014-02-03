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
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.configuration.Configuration;

import com.splout.db.common.CompressorUtil;
import com.splout.db.common.PortUtils;
import com.splout.db.common.PortUtils.PortLock;
import com.splout.db.engine.EmbeddedMySQL.EmbeddedMySQLConfig;

public class MySQLManager implements EngineManager {

	private EmbeddedMySQL mySQL;
	private JDBCManager jdbcManager;

	public MySQLManager() throws SQLException, ClassNotFoundException {

	}

	// Only to be used from unit testing
	MySQLManager(EmbeddedMySQL mySQL) throws SQLException, ClassNotFoundException {
		initializeMySQL(mySQL.getConfig());
	}
	
	@Override
	public void close() {
		jdbcManager.close();
		if(mySQL != null)
			mySQL.stop();
	}

	JDBCManager getJdbcManager() {
	  return jdbcManager;
  }
	
	@Override
	public String exec(String query) throws EngineException {
		return jdbcManager.exec(query);
	}

	@Override
	public String query(String query, int maxResults) throws EngineException {
		return jdbcManager.query(query, maxResults);
	}

	private void initializeMySQL(EmbeddedMySQLConfig mysqlConfig) throws SQLException, ClassNotFoundException {
		this.jdbcManager = new JDBCManager(EmbeddedMySQL.DRIVER, mysqlConfig.getLocalJDBCConnection(MySQLOutputFormat.GENERATED_DB_NAME), 1,
		    mysqlConfig.getUser(), mysqlConfig.getPass());
	}
	
	@Override
	public void init(File dbFile, Configuration config, List<String> initStatements)
	    throws EngineException {

		File dbFolder = dbFile.getParentFile();
		File mysqlFolder = new File(dbFolder, "mysql");

		PortLock portLock = PortUtils.getNextAvailablePort(EmbeddedMySQLConfig.DEFAULT_PORT);
		try {
			EmbeddedMySQLConfig mysqlConfig = new EmbeddedMySQLConfig(portLock.getPort(),
			    EmbeddedMySQLConfig.DEFAULT_USER, EmbeddedMySQLConfig.DEFAULT_PASS, mysqlFolder, null);
			mySQL = new EmbeddedMySQL(mysqlConfig);
			// Trick: start mysql first on the empty dir, stop it, uncompress data, start it again
			// This is because mySQL creates some databases by default which doesn't create if "data" already exists
			// So we don't need to add them to the produced zip (1.6 MB less).
			mySQL.start(true);
			mySQL.stop();

			CompressorUtil.uncompress(dbFile, mysqlFolder);

			mySQL.start(false);

			initializeMySQL(mysqlConfig);
		} catch(IOException e) {
			throw new EngineException(e);
		} catch(InterruptedException e) {
			throw new EngineException(e);
		} catch(SQLException e) {
			throw new EngineException(e);
		} catch(ClassNotFoundException e) {
			throw new EngineException(e);
		} finally {
			portLock.release();
		}
	}
}
