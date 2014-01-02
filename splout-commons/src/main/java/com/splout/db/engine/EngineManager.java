package com.splout.db.engine;

import java.io.File;
import java.util.List;

import org.apache.commons.configuration.Configuration;

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

/**
 * Contract for implementing several engine interfaces.
 */
public interface EngineManager {

	@SuppressWarnings("serial")
	public static class EngineException extends Exception {

		public EngineException(Throwable underlying) {
			super(underlying);
		}

		public EngineException(String message, Throwable underlying) {
			super(message, underlying);
		}
	}

	public void init(File dbFile, Configuration config, List<String> initStatements) throws EngineException;
	
	/**
	 * SQL command, returning JSON object status result. This is implementation-dependent with no particular constraints.
	 */
	public String exec(String query) throws EngineException;

	/**
	 * SQL query, returning JSON result (see {@link JDBCManager.#convertResultSetToList(java.sql.ResultSet, int)}) as an
	 * example. This should be implementation-independent. It should return a JSONized List<Map<String, Object>>.
	 */
	public String query(String query, int maxResults) throws EngineException;

	public void close();
}
