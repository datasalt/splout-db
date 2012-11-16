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

/**
 * Use this class to quickly run "n" DNodes in localhost.
 */
public class DNodeEnsemble {

	public void runForever(int nDNodes) throws Exception {
		SploutConfiguration config = SploutConfiguration.getTestConfig();
		for(int i = 0; i < nDNodes; i++) {
			config = SploutConfiguration.getTestConfig();
			// we need to change some props for avoiding conflicts, ports, etc
			config.setProperty(DNodeProperties.PORT, config.getInt(DNodeProperties.PORT) + i);
			config.setProperty(DNodeProperties.DATA_FOLDER, config.getString(DNodeProperties.DATA_FOLDER) + "-" + i);
			config.setProperty(FetcherProperties.TEMP_DIR, config.getString(FetcherProperties.TEMP_DIR) + "-" + i);
			DNode dnode = new DNode(config, new DNodeHandler());
			dnode.init();
		}
	}

	public static void main(String[] args) throws Exception {
		DNodeEnsemble ensemble = new DNodeEnsemble();
		ensemble.runForever(Integer.parseInt(args[0]));
	}
}