package com.splout.db.common;

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
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.FileUtils;
import org.apache.thrift.transport.TTransportException;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.splout.db.common.JSONSerDe.JSONSerDeException;
import com.splout.db.dnode.DNode;
import com.splout.db.dnode.DNodeProperties;
import com.splout.db.dnode.FetcherProperties;
import com.splout.db.dnode.IDNodeHandler;
import com.splout.db.engine.SQLite4JavaManager;
import com.splout.db.engine.EngineManager.EngineException;
import com.splout.db.hazelcast.CoordinationStructures;
import com.splout.db.hazelcast.HazelcastConfigBuilder;
import com.splout.db.hazelcast.HazelcastConfigBuilder.HazelcastConfigBuilderException;
import com.splout.db.qnode.IQNodeHandler;
import com.splout.db.qnode.QNode;
import com.splout.db.qnode.QNodeProperties;

/**
 * Things that are used extensively in unit / integration tests.
 */
public class TestUtils {

	/**
	 * Creates a simple database with two columns: one integer (a) and one string (b). It also insertes one default row
	 * from parameters a, b.
	 * @throws EngineException 
	 */
	public static void createFooDatabase(String where, int a, String b) throws SQLException, JSONSerDeException,
	    ClassNotFoundException, EngineException {
		File dbFolder = new File(where);
		dbFolder.mkdir();
		final SQLite4JavaManager manager = new SQLite4JavaManager();
		manager.init(new File(where + "/" + "foo.db"), null, null);
		manager.query("DROP TABLE IF EXISTS t;", 100);
		manager.query("CREATE TABLE t (a INT, b TEXT);", 100);
		manager.query("INSERT INTO t (a, b) VALUES (" + a + ", \"" + b + "\")", 100);
		manager.close();
	}

	/**
	 * Use this method to get a high-level client for Hazelcast in unit tests.
	 * @throws HazelcastConfigBuilderException 
	 */
	public static CoordinationStructures getCoordinationStructures(SploutConfiguration testConfig) throws HazelcastConfigBuilderException {
		HazelcastInstance hz = Hazelcast.newHazelcastInstance(HazelcastConfigBuilder.build(testConfig));
		CoordinationStructures coord = new CoordinationStructures(hz);
		return coord;
	}
	
	/**
	 * Use this class for waiting on a certain condition up to some time.
	 */
	public static abstract class NotWaitingForeverCondition {
		
		public abstract boolean endCondition();
		
		public void waitAtMost(long patience) throws InterruptedException {
			long waitedSoFar = 0;
			while(!endCondition()) {
				Thread.sleep(200);
				waitedSoFar += 200;
				if(waitedSoFar > patience) {
					throw new AssertionError("Waited more than " + patience + " on a test condition.");
				}
			}
		}
	}
	
	/**
	 * Utility class that can be used for implementing things that can be retried up to a maximum number of times.
	 */
	public static abstract class CatchAndRetry {

		private int maxRetrials;
		private Class<? extends Throwable> exception;

		public CatchAndRetry(Class<? extends Throwable> exception, int maxRetrials) {
			this.exception = exception;
			this.maxRetrials = maxRetrials;
		}

		public abstract void businessLogic() throws Throwable;
		public abstract void retryLogic();
		
		public void catchAndRetry() throws Throwable {
			int trial = 0;
			boolean succeeded = false;
			do {
				try {
					businessLogic();
					succeeded = true;
				} catch(Throwable t) {
					if(exception.isAssignableFrom(t.getClass())) {
						trial++;
						retryLogic();
					} else {
						throw t;
					}
				}
			} while(!succeeded && trial < maxRetrials);
		}
	}
	
	/**
	 * Returns a QNode instance if, after a maximum of X trials, we can find a port to bind it to.
	 * The configuration passed by instance might have been modified accordingly.
	 */
	public static QNode getTestQNode(final SploutConfiguration testConfig, final IQNodeHandler handler) throws Throwable {
		final AtomicReference<QNode> reference = new AtomicReference<QNode>();
		CatchAndRetry qNodeInit = new CatchAndRetry(java.net.BindException.class, 50) {

			@Override
      public void businessLogic() throws Throwable {
				QNode qNode = new QNode();
				qNode.start(testConfig, handler);
				reference.set(qNode);
			}

			@Override
      public void retryLogic() {
				testConfig.setProperty(QNodeProperties.PORT, testConfig.getInt(QNodeProperties.PORT) + 1);
			}
		};
		qNodeInit.catchAndRetry();
		return reference.get();
	}
	
	public static DNode getTestDNode(final SploutConfiguration testConfig, final IDNodeHandler handler, final String dataFolder) throws Throwable {
		return getTestDNode(testConfig, handler, dataFolder, true);
	}
	
	/**
	 * Returns a DNode instance if, after a maximum of X trials, we can find a port to bind it to.
	 * The configuration passed by instance might have been modified accordingly.
	 */
	public static DNode getTestDNode(final SploutConfiguration testConfig, final IDNodeHandler handler, final String dataFolder, boolean deleteDataFolder) throws Throwable {
		final AtomicReference<DNode> reference = new AtomicReference<DNode>();
		testConfig.setProperty(DNodeProperties.DATA_FOLDER, dataFolder);
		if(deleteDataFolder) {
			File file = new File(dataFolder);
			if(file.exists()) {
				FileUtils.deleteDirectory(file);
			}
			file.mkdir();
		}
		testConfig.setProperty(FetcherProperties.TEMP_DIR, "fetcher-" + dataFolder);
		File fetcherTmp = new File("fetcher-" + dataFolder);
		if(fetcherTmp.exists()) {
			FileUtils.deleteDirectory(fetcherTmp);
		}
		fetcherTmp.mkdir();
		CatchAndRetry dNodeInit = new CatchAndRetry(TTransportException.class, 50) {

			@Override
      public void businessLogic() throws Throwable {
				DNode dNode = new DNode(testConfig, handler);
				dNode.init();
				reference.set(dNode);
			}
			
			@Override
			public void retryLogic() {
				testConfig.setProperty(DNodeProperties.PORT, testConfig.getInt(DNodeProperties.PORT) + 1);
			}
		};
		dNodeInit.catchAndRetry();
		return reference.get();
	}

	/**
	 * Delete folders that *might* have been created by DNodes & ZooKeepers from a certain test class.
	 * The number of instances is the number of different methods (and therefore unique namespaces) in the test class.
	 * The convention is: dnode-ClassName-i for Dnode data folder of test class "className" and method "i".
	 * Same follows for ZooKeeper data folders "zk-" and Fetcher TMP folders "fetcher-dnode-" .
	 */
	public static void cleanUpTmpFolders(String className, int nInstances) throws IOException {
		File file = new File("dnode-" + className);
		if(file.exists()) {
			FileUtils.deleteDirectory(file);
		}
		file = new File("zk-" + className);
		if(file.exists()) {
			FileUtils.deleteDirectory(file);
		}
		file = new File("fetcher-dnode-" + className);
		if(file.exists()) {
			FileUtils.deleteDirectory(file);
		}
		for(int i = 0; i <= nInstances; i++) {
			file = new File("dnode-" + className + "-" + i);
			if(file.exists()) {
				FileUtils.deleteDirectory(file);
			}
			file = new File("zk-" + className + "-" + i);
			if(file.exists()) {
				FileUtils.deleteDirectory(file);
			}
			file = new File("fetcher-dnode-" + className + "-" + i);
			if(file.exists()) {
				FileUtils.deleteDirectory(file);
			}
		}
	}

}
