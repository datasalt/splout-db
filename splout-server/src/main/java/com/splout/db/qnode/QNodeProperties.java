package com.splout.db.qnode;

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
 * All the {@link com.splout.db.common.SploutConfiguration} properties related to the {@link QNode}.
 */
public class QNodeProperties {

	/**
	 * The port this QNode will run on
	 */
	public final static String PORT = "qnode.port";
	/**
	 * The host this QNode will run on
	 */
	public final static String HOST = "qnode.host";
	/**
	 * Number of Thrift connections allocated as a connection pool in each QNode.
	 */
	public final static String DNODE_POOL_SIZE = "qnode.dnode.pool.size";
	/**
	 * Whether this QNode should find the next available port in case "dnode.port" is busy or fail otherwise.
	 */
	public final static String PORT_AUTOINCREMENT = "qnode.port.autoincrement";
	/**
	 * The number of succeessfully deployed versions that will be kept in the system (per tablespace)
	 */
	public final static String VERSIONS_PER_TABLESPACE = "qnode.versions.per.tablespace";
	/**
	 * The timeout for a global deploy in seconds. -1 for disabling timeout
	 */
	public final static String DEPLOY_TIMEOUT = "qnode.deploy.timeout";
	/**
	 * The number of seconds to wait before checking each time if a DNode has failed or if timeout has ocurred in the middle of a deploy
	 */
	public final static String DEPLOY_SECONDS_TO_CHECK_ERROR = "qnode.deploy.seconds.to.check.error";
}
