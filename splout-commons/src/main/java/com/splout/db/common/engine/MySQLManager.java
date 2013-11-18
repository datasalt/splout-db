package com.splout.db.common.engine;

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

import java.sql.SQLException;

import com.splout.db.common.engine.EmbeddedMySQL.EmbeddedMySQLConfig;

public class MySQLManager extends JDBCManager {

	public static final String DRIVER = "com.mysql.jdbc.Driver";
	
	public MySQLManager(EmbeddedMySQLConfig config, String dbName, int nConnections) throws SQLException, ClassNotFoundException {
		super(DRIVER, config.getLocalJDBCConnection(dbName), nConnections, config.getUser(), config.getPass());
	}

}
