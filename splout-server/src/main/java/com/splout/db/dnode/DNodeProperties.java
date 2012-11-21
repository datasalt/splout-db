package com.splout.db.dnode;

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

/**
 * All the {@link com.splout.db.common.SploutConfiguration} properties related to the {@link DNode}.
 */
public class DNodeProperties {

	/**
	 * The data folder that will be used for storing deployed SQL data stores.
	 */
	public final static String DATA_FOLDER = "dnode.data.folder";
	/**
	 * This DNode's host name
	 */
	public final static String HOST = "dnode.host";
	/**
	 * This DNode's port
	 */
	public final static String PORT = "dnode.port";
	/**
	 * How many threads will be allocated for serving requests in Thrift's ThreadPoolServer
	 */
	public final static String SERVING_THREADS = "dnode.serving.threads";
	/**
	 * Whether this DNode should find the next available port in case "dnode.port" is busy or fail otherwise.
	 */
	public final static String PORT_AUTOINCREMENT = "dnode.port.autoincrement";
	/**
	 * The amount of seconds that the DNode will cache SQL connection pools. After that time, it will close them. Remember
	 * that the DNode may receive requests for different versions in the middle of a deployment, so that's why we want to
	 * expire connection pools after some time (to not cache connection pools that will not be used anymore).
	 */
	public final static String EH_CACHE_SECONDS = "dnode.pool.cache.seconds";
	/**
	 * The amount of seconds that the DNode will wait before canceling a too-long deployment.
	 */
	public final static String DEPLOY_TIMEOUT_SECONDS = "dnode.deploy.timeout.seconds";
	/**
	 * A hard limit on the number of results per each SQL query that this DNode may send back to QNodes.
	 */
	public final static String MAX_RESULTS_PER_QUERY = "dnode.max.results.per.query";
	/**
	 * If set, this DNode will listen for test commands. This property is used to activate responsiveness to some commands
	 * that are useful for integration testing: making a DNode shutdown, etc.
	 */
	public final static String HANDLE_TEST_COMMANDS = "dnode.handle.test.commands";
	/**
	 * Number of SQL connection pools that will be cached. There will be one SQL connection pool for each tablespace,
	 * version and partition that this DNode serves. So this number must not be smaller than the different numbers of
	 * tablespace + version + partitions.
	 */
	public final static String EH_CACHE_N_ELEMENTS = "dnode.pool.cache.n.elements";
	/**
	 * In milliseconds, queries that are slower will be logged with a WARNING. 
	 */
	public final static String SLOW_QUERY_ABSOLUTE_LIMIT = "dnode.slow.query.abs.limit";
}