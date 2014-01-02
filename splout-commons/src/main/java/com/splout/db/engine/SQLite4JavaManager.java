package com.splout.db.engine;

/*
 * #%L
 * Splout SQL commons
 * %%
 * Copyright (C) 2012 Datasalt Systems S.L.
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
