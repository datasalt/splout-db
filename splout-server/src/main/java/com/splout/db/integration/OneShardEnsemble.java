package com.splout.db.integration;

/*
 * #%L
 * Splout SQL Server
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

import com.splout.db.common.SploutConfiguration;
import com.splout.db.dnode.DNode;
import com.splout.db.dnode.DNodeHandler;
import com.splout.db.qnode.QNode;
import com.splout.db.qnode.QNodeHandler;

/**
 * A program for running one QNode and one DNode with all default configurations.
 */
public class OneShardEnsemble {

  public void runForever() throws Exception {
    SploutConfiguration config = SploutConfiguration.getTestConfig();
    DNode dnode = new DNode(config, new DNodeHandler());
    dnode.init();
    QNode qnode = new QNode();
    qnode.start(config, new QNodeHandler());
  }

  public static void main(String[] args) throws Exception {
    OneShardEnsemble ensemble = new OneShardEnsemble();
    ensemble.runForever();
  }
}
