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
import com.splout.db.dnode.DNodeProperties;
import com.splout.db.dnode.FetcherProperties;
import com.splout.db.qnode.QNode;
import com.splout.db.qnode.QNodeHandler;

/**
 * Like {@link OneShardEnsemble}, but for any number of local DNodes.
 */
public class NShardEnsemble {

	public void runForever(int nDNodes) throws Exception {
		SploutConfiguration config = SploutConfiguration.get();
		for(int i = 0; i < nDNodes; i++) {
			config = SploutConfiguration.get();
			// we need to change some props for avoiding conflicts, ports, etc
			config.setProperty(DNodeProperties.PORT, config.getInt(DNodeProperties.PORT) + i);
			config.setProperty(DNodeProperties.DATA_FOLDER, config.getString(DNodeProperties.DATA_FOLDER) + "-" + i);
			config.setProperty(FetcherProperties.TEMP_DIR, config.getString(FetcherProperties.TEMP_DIR) + "-" + i);
			DNode dnode = new DNode(config, new DNodeHandler());
			dnode.init();
		}
		QNode qnode = new QNode();
		qnode.start(config, new QNodeHandler());
	}

	public static void main(String[] args) throws Exception {
		NShardEnsemble ensemble = new NShardEnsemble();
		ensemble.runForever(Integer.parseInt(args[0]));
	}
}
