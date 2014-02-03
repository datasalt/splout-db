package com.splout.db.engine;

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

import java.io.File;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.configuration.Configuration;

import com.splout.db.common.TimeoutThread;

/**
 *
 */
public class SQLite4JavaManager implements EngineManager {

	private SQLite4JavaClient client;
	
	public SQLite4JavaManager() {
		
	}
	
	SQLite4JavaManager(String dbFile, List<String> initStatements) {
		this.client = new SQLite4JavaClient(dbFile, initStatements);
	}
	
	public void setTimeoutThread(TimeoutThread t) {
		client.setTimeoutThread(t);
	}
	
	@Override
  public void init(File dbFile, Configuration config, List<String> initStatements) throws EngineException {
		this.client = new SQLite4JavaClient(dbFile + "", initStatements);
	}

	@Override
  public String exec(String query) throws EngineException {
	  try {
	    return client.exec(query);
    } catch(SQLException e) {
    	throw new EngineException(e);
    }
  }

	@Override
  public String query(String query, int maxResults) throws EngineException {
	  try {
	    return client.query(query, maxResults);
    } catch(SQLException e) {
	    throw new EngineException(e);
    }
  }

	@Override
  public void close() {
		client.close();
  }
}
