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

import com.splout.db.common.SploutConfiguration;
import com.splout.db.common.TimeoutThread;
import com.splout.db.dnode.DNodeProperties;
import com.splout.db.thrift.PartitionMetadata;

import java.io.File;

/**
 * Stateful factory where engine-specific business logic for instantiating a {@link EngineManager} for each
 * {@link SploutEngine} is written. The DNode calls this factory at startup, closing time and everytime it needs to open a new
 * manager for a new data partition with associated {@link PartitionMetadata}.
 */
public class ManagerFactory {

  // private final static Log log = LogFactory.getLog(ManagerFactory.class);
  private SploutConfiguration config;
  private TimeoutThread timeoutThread;

  public void init(SploutConfiguration config) {
    this.config = config;
    timeoutThread = new TimeoutThread(config.getLong(DNodeProperties.MAX_QUERY_TIME));
    timeoutThread.start();
  }

  public void close() {
    timeoutThread.interrupt();
  }

  public EngineManager getManagerIn(File dbFolder, PartitionMetadata partitionMetadata) throws Exception {
    EngineManager manager = null;

    SploutEngine engine = SploutEngine.getDefault();
    // Assume default engine in case of no engine (to preserve backwards compatibility)
    if (partitionMetadata.getEngineId() != null) {
      engine = Class.forName(partitionMetadata.getEngineId()).asSubclass(SploutEngine.class)
          .newInstance();
    }
    manager = Class.forName(engine.getEngineManagerClass()).asSubclass(EngineManager.class)
        .newInstance();

    // Currently using first ".db" file found (there should be only one!)
    String dbFile = null;
    for (String file : dbFolder.list()) {
      if (file.endsWith(".db")) {
        dbFile = file;
        break;
      }
    }
    if (dbFile == null) {
      throw new RuntimeException("Can't find .db file in directory: " + dbFolder);
    }

    File absoluteDbFile = new File(dbFolder + "/" + dbFile);
    manager.init(absoluteDbFile, config, partitionMetadata.getInitStatements());

    if (manager instanceof SQLite4JavaManager) {
      ((SQLite4JavaManager) manager).setTimeoutThread(timeoutThread);
    }

    return manager;
  }
}
