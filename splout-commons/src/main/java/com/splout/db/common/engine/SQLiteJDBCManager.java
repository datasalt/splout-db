package com.splout.db.common.engine;

/*
 * #%L
 * Splout SQL commons
 * %%
 * Copyright (C) 2012 Datasalt Systems S.L.
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

/**
 * SQL Wrapper for querying SQLite through a connection pool using BoneCP (<a
 * href="http://jolbox.com">http://jolbox.com/</a>).
 */
public class SQLiteJDBCManager extends JDBCManager {

	public final static String DRIVER = "org.sqlite.JDBC";
	
	public SQLiteJDBCManager(String dbFile, int nConnections) throws SQLException, ClassNotFoundException {
		super(DRIVER, "jdbc:sqlite:" + dbFile, nConnections, "foo", "foo");
	}

}