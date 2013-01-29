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

import java.util.List;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;

import com.splout.db.common.SploutConfiguration;
import com.splout.db.thrift.DNodeException;
import com.splout.db.thrift.DNodeService;
import com.splout.db.thrift.DeployAction;
import com.splout.db.thrift.RollbackAction;
import com.splout.db.thrift.TablespaceVersion;

/**
 * The Thrift skeleton for the DNode service. This class only implements the Thrift logic.
 * <p>
 * The DNode's own business logic is packed into {@link DNodeHandler} which is an implementation of
 * {@link IDNodeHandler}. In this way, we can use the DNode in unit tests by using Mock implementations of
 * {@link IDNodeHandler}.
 */
public class DNode implements DNodeService.Iface {

	private final static Log log = LogFactory.getLog(DNode.class);

	private IDNodeHandler handler;
	private TServer server;
	private Thread servingThread;
	private SploutConfiguration config;

	public DNode(SploutConfiguration config, IDNodeHandler handler) {
		this.handler = handler;
		this.config = config;
	}

	/**
	 * Returns the address (host:port) of this DNode.
	 */
	public String getAddress() {
		return config.getString(DNodeProperties.HOST) + ":" + config.getInt(DNodeProperties.PORT);
	}

	/**
	 * Initialize the DNode service - the important thing here is to instantiate a multi-threaded Thrift server.
	 * Everything is based on the given {@link SploutConfiguration} by constructor.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void init() throws Exception {
		DNodeService.Processor processor = new DNodeService.Processor(this);
		TNonblockingServerTransport serverTransport = null;

		boolean init = false;
		int retries = 0;
		int thriftPort;
		
		do {
			thriftPort = config.getInt(DNodeProperties.PORT);
			try {
				serverTransport = new TNonblockingServerSocket(thriftPort);
				init = true;
			} catch(org.apache.thrift.transport.TTransportException e) {
				if(!config.getBoolean(DNodeProperties.PORT_AUTOINCREMENT)) {
					throw e;
				}
				config.setProperty(DNodeProperties.PORT, thriftPort + 1);
				retries++;
			}
		} while(!init && retries < 100);
		
		handler.init(config);

		THsHaServer.Args args = new THsHaServer.Args(serverTransport);
		args.executorService(Executors.newFixedThreadPool(config.getInt(DNodeProperties.SERVING_THREADS)));
		args.processor(processor);
		
		server = new THsHaServer(args);
		// We instantiate a long-living serving thread that will use the Thrift server.
		servingThread = new Thread("Serving Thread") {
			public void run() {
				try {
					server.serve();
				} catch(Throwable t) {
					t.printStackTrace();
				}
			};
		};
		servingThread.start();
		handler.giveGreenLigth();
		log.info("Thrift server started on port: " + thriftPort);
	}

	// ---- The following methods are a facade for {@link IDNodeHandler} ---- //

	@Override
	public String sqlQuery(String tablespace, long version, int partition, String query)
	    throws DNodeException, TException {
		return handler.sqlQuery(tablespace, version, partition, query);
	}

	@Override
	public String deploy(List<DeployAction> deployActions, long version) throws DNodeException, TException {
		return handler.deploy(deployActions, version);
	}

	@Override
	public String rollback(List<RollbackAction> rollbackActions, String distributedBarrier)
	    throws DNodeException, TException {
		return handler.rollback(rollbackActions, distributedBarrier);
	}

	@Override
	public String status() throws DNodeException, TException {
		return handler.status();
	}
	
	

	public void stop() throws Exception {
		handler.stop();
		server.stop();
		servingThread.join();
	}

	public static void main(String[] args) throws Exception {
		SploutConfiguration config;
		if(args.length == 1) {
			// config root
			config = SploutConfiguration.get(args[0]);
		} else {
			config = SploutConfiguration.get();
		}
		DNode dnode = new DNode(config, new DNodeHandler());
		dnode.init();
	}

	@Override
	public String abortDeploy(long version) throws DNodeException, TException {
		return handler.abortDeploy(version);
	}

	@Override
	public String deleteOldVersions(List<TablespaceVersion> versions) throws DNodeException, TException {
		return handler.deleteOldVersions(versions);
	}

	@Override
  public String testCommand(String command) throws DNodeException, TException {
	  return handler.testCommand(command);
  }

	@Override
  public String createTablespacePartitions(List<DeployAction> deployActions, long version)
      throws DNodeException, TException {
	  return handler.createTablespacePartitions(deployActions, version);
  }
}