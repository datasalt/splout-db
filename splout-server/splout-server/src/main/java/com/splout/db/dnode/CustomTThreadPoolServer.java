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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * TThreadPoolServer (approximately) patched with:
 * https://issues.apache.org/jira/secure/attachment/12444333/THRIFT-692.patch.untested.txt
 */
public class CustomTThreadPoolServer extends TServer {

	private static final Log LOGGER = LogFactory.getLog(CustomTThreadPoolServer.class.getName());

	public static class Args extends AbstractServerArgs<Args> {
		public int minWorkerThreads = 5;
		public int maxWorkerThreads = Integer.MAX_VALUE;
		public int stopTimeoutVal = 60;
		public TimeUnit stopTimeoutUnit = TimeUnit.SECONDS;

		public Args(TServerTransport transport) {
			super(transport);
		}

		public Args minWorkerThreads(int n) {
			minWorkerThreads = n;
			return this;
		}

		public Args maxWorkerThreads(int n) {
			maxWorkerThreads = n;
			return this;
		}
	}

	// Executor service for handling client connections
	private ExecutorService executorService_;

	// Flag for stopping the server
	private volatile boolean stopped_;

	private final TimeUnit stopTimeoutUnit;

	private final long stopTimeoutVal;

	public CustomTThreadPoolServer(Args args) {
		super(args);

		LinkedBlockingQueue<Runnable> executorQueue = new LinkedBlockingQueue<Runnable>(
		    args.maxWorkerThreads);

		stopTimeoutUnit = args.stopTimeoutUnit;
		stopTimeoutVal = args.stopTimeoutVal;

		executorService_ = new ThreadPoolExecutor(args.minWorkerThreads, args.maxWorkerThreads, 60,
		    TimeUnit.SECONDS, executorQueue);
	}

	public void serve() {
		try {
			serverTransport_.listen();
		} catch(TTransportException ttx) {
			LOGGER.error("Error occurred during listening.", ttx);
			return;
		}

		stopped_ = false;
		setServing(true);
		while(!stopped_) {
			int failureCount = 0;
			try {
				TTransport client = serverTransport_.accept();
				WorkerProcess wp = new WorkerProcess(client);
				try {
					executorService_.execute(wp);
				} catch(RejectedExecutionException ree) {
					++failureCount;
					LOGGER.warn("Execution rejected.", ree);
					client.close();
				}
			} catch(TTransportException ttx) {
				if(!stopped_) {
					++failureCount;
					LOGGER.warn("Transport error occurred during acceptance of message.", ttx);
				}
			} catch(Error e) {
				if(!stopped_) {
					++failureCount;
					LOGGER.warn("Uncaught error.", e);
				}
			}
		}

		executorService_.shutdown();

		// Loop until awaitTermination finally does return without a interrupted
		// exception. If we don't do this, then we'll shut down prematurely. We want
		// to let the executorService clear it's task queue, closing client sockets
		// appropriately.
		long timeoutMS = stopTimeoutUnit.toMillis(stopTimeoutVal);
		long now = System.currentTimeMillis();
		while(timeoutMS >= 0) {
			try {
				executorService_.awaitTermination(timeoutMS, TimeUnit.MILLISECONDS);
				break;
			} catch(InterruptedException ix) {
				long newnow = System.currentTimeMillis();
				timeoutMS -= (newnow - now);
				now = newnow;
			}
		}
		setServing(false);
	}

	public void stop() {
		stopped_ = true;
		serverTransport_.interrupt();
	}

	private class WorkerProcess implements Runnable {

		/**
		 * Client that this services.
		 */
		private TTransport client_;

		/**
		 * Default constructor.
		 * 
		 * @param client
		 *          Transport to process
		 */
		private WorkerProcess(TTransport client) {
			client_ = client;
		}

		/**
		 * Loops on processing a client forever
		 */
		public void run() {
			TProcessor processor = null;
			TTransport inputTransport = null;
			TTransport outputTransport = null;
			TProtocol inputProtocol = null;
			TProtocol outputProtocol = null;
			try {
				processor = processorFactory_.getProcessor(client_);
				inputTransport = inputTransportFactory_.getTransport(client_);
				outputTransport = outputTransportFactory_.getTransport(client_);
				inputProtocol = inputProtocolFactory_.getProtocol(inputTransport);
				outputProtocol = outputProtocolFactory_.getProtocol(outputTransport);
				// we check stopped_ first to make sure we're not supposed to be shutting
				// down. this is necessary for graceful shutdown.
				while(!stopped_ && processor.process(inputProtocol, outputProtocol)) {
				}
			} catch(TTransportException ttx) {
				// Assume the client died and continue silently
			} catch(TException tx) {
				LOGGER.error("Thrift error occurred during processing of message.", tx);
			} catch(Exception x) {
				LOGGER.error("Error occurred during processing of message.", x);
			}

			if(inputTransport != null) {
				inputTransport.close();
			}

			if(outputTransport != null) {
				outputTransport.close();
			}
		}
	}
}