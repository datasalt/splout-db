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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;

import org.apache.commons.io.FileSystemUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICountDownLatch;
import com.splout.db.benchmark.PerformanceTool;
import com.splout.db.common.JSONSerDe;
import com.splout.db.common.JSONSerDe.JSONSerDeException;
import com.splout.db.common.SQLite4JavaManager;
import com.splout.db.common.SploutConfiguration;
import com.splout.db.common.ThriftReader;
import com.splout.db.common.ThriftWriter;
import com.splout.db.common.TimeoutThread;
import com.splout.db.dnode.beans.DNodeStatusResponse;
import com.splout.db.dnode.beans.DNodeSystemStatus;
import com.splout.db.hazelcast.CoordinationStructures;
import com.splout.db.hazelcast.DNodeInfo;
import com.splout.db.hazelcast.DistributedRegistry;
import com.splout.db.hazelcast.HazelcastConfigBuilder;
import com.splout.db.hazelcast.HazelcastConfigBuilder.HazelcastConfigBuilderException;
import com.splout.db.hazelcast.HazelcastProperties;
import com.splout.db.thrift.DNodeException;
import com.splout.db.thrift.DeployAction;
import com.splout.db.thrift.PartitionMetadata;
import com.splout.db.thrift.RollbackAction;

/**
 * The business logic for the DNode: responding to queries, downloading new deployments, handling ZooKeeper events and
 * so forth.
 */
public class DNodeHandler implements IDNodeHandler {

	private final static Log log = LogFactory.getLog(DNodeHandler.class);

	private SploutConfiguration config;
	private HazelcastInstance hz;
	private DistributedRegistry dnodesRegistry;
	private CoordinationStructures coord;

	// The {@link Fetcher} is the responsible for downloading new deployment data.
	Cache dbCache;

	protected ExecutorService deployThread;
	protected Object deployLock = new Object();

	// This flag is needed for unit testing.
	protected AtomicInteger deployInProgress = new AtomicInteger(0);
	// Indicates that the last deploy failed because of timeout. This info can then be answered via a status() request.
	AtomicBoolean lastDeployTimedout = new AtomicBoolean(false);

	// Thrift exception code used in DNodeException
	public final static int EXCEPTION_ORDINARY = 0;
	public final static int EXCEPTION_UNEXPECTED = 1;

	// A hard limit on the number of results that this DNode can return per SQL query
	private int maxResultsPerQuery;

	// The following variables are used for monitoring and providing statistics:
	private PerformanceTool performanceTool = new PerformanceTool();
	private String lastException = null;
	private long lastExceptionTime;
	private AtomicInteger failedQueries = new AtomicInteger(0);
	private long upSince;

	// The {@link Fetcher} is the responsible for downloading new deployment data.
	private Fetcher fetcher;
	
	// Above this query time the query will be logged as slow query
	private long absoluteSlowQueryLimit;
	
	// This thread will interrupt long-running queries
	private TimeoutThread timeoutThread;
	
	public DNodeHandler(Fetcher fetcher) {
		this.fetcher = fetcher;
	}
	
	public DNodeHandler() {
	}

	/**
	 * Returns the address (host:port) of this DNode.
	 */
	public String whoAmI() {
		return config.getString(DNodeProperties.HOST) + ":" + config.getInt(DNodeProperties.PORT);
	}

	/**
	 * Initialization logic: initialize things, connect to ZooKeeper, create Thrift server, etc.
	 * 
	 * @see com.splout.db.dnode.IDNodeHandler#init(com.splout.db.common.SploutConfiguration)
	 */
	public void init(SploutConfiguration config) throws Exception {
		this.config = config;
		long evictionSeconds = config.getLong(DNodeProperties.EH_CACHE_SECONDS);
		maxResultsPerQuery = config.getInt(DNodeProperties.MAX_RESULTS_PER_QUERY);
		int maxCachePools = config.getInt(DNodeProperties.EH_CACHE_N_ELEMENTS);
		absoluteSlowQueryLimit = config.getLong(DNodeProperties.SLOW_QUERY_ABSOLUTE_LIMIT);
		timeoutThread = new TimeoutThread(config.getLong(DNodeProperties.MAX_QUERY_TIME));
		// We create a Cache for holding SQL connection pools to different tablespace versions
		// http://stackoverflow.com/questions/2583429/how-to-differentiate-between-time-to-live-and-time-to-idle-in-ehcache
		dbCache = new Cache("dbCache", maxCachePools, false, false, Integer.MAX_VALUE, evictionSeconds);
		dbCache.initialise();
		if(fetcher == null) {
			// The Fetcher in charge of downloading new deployments
			this.fetcher = new Fetcher(config);
		}
		// When a tablespace version is expired, the connection pool is closed by an expiration handler
		dbCache.getCacheEventNotificationService().registerListener(new CacheListener());
		// The thread that will execute deployments asynchronously
		deployThread = Executors.newFixedThreadPool(1);
		// Connect with the cluster.
		hz = Hazelcast.newHazelcastInstance(HazelcastConfigBuilder.build(config));
		coord = new CoordinationStructures(hz);
		// Add shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				try {
					log.info("Shutdown hook called - trying to gently stop DNodeHandler " + whoAmI() + " ...");
					DNodeHandler.this.stop();
				} catch(Throwable e) {
					log.error("Error in ShutdownHook", e);
				}
			}
		});
		upSince = System.currentTimeMillis();
	}

	/**
	 * Registers the dnode in the cluster. This gives green ligth to use it.
	 */
	@Override
	public void giveGreenLigth() {
		int minutesToCheckRegister = config.getInt(HazelcastProperties.MAX_TIME_TO_CHECK_REGISTRATION, 5);
		int oldestMembersLeading = config.getInt(HazelcastProperties.OLDEST_MEMBERS_LEADING_COUNT, 3);

		dnodesRegistry = new DistributedRegistry(CoordinationStructures.DNODES, new DNodeInfo(config), hz,
		    minutesToCheckRegister, oldestMembersLeading);
		dnodesRegistry.register();
	}

	/**
	 * Deletes the files and folders kept by the DNode for a particular tablespace and version.
	 */
	private void deleteLocalVersion(com.splout.db.thrift.TablespaceVersion version) throws IOException {
		File dataFolder = new File(config.getString(DNodeProperties.DATA_FOLDER));
		File tablespaceFolder = new File(dataFolder, version.getTablespace());
		File versionFolder = new File(tablespaceFolder, version.getVersion() + "");
		if(versionFolder.exists()) {
			FileUtils.deleteDirectory(versionFolder);
			log.info("-- Successfully removed " + versionFolder);
		} else {
			// Could happen, nothing to worry
		}
	}

	/**
	 * Thrift RPC method -> Given a tablespace and a version, execute the SQL query
	 */
	@Override
	public String sqlQuery(String tablespace, long version, int partition, String query)
	    throws DNodeException {
		
		try {
			try {
				performanceTool.startQuery();
				// Look for the EHCache database pool cache
				String dbKey = tablespace + "_" + version + "_" + partition;
				Element dbPoolInCache = null;
				synchronized(dbCache) {
					dbPoolInCache = dbCache.get(dbKey);
					if(dbPoolInCache == null) {
						File dbFolder = getLocalStorageFolder(tablespace, partition, version);
						if(!dbFolder.exists()) {
							log.warn("Asked for " + dbFolder + " but it doesn't exist!");
							throw new DNodeException(EXCEPTION_ORDINARY, "Requested tablespace (" + tablespace
							    + ") + version (" + version + ") is not available.");
						}
						// Currently using first ".db" file but in the future there might be some convention
						for(String file : dbFolder.list()) {
							if(file.endsWith(".db")) {
								// Create new EHCache item value with a {@link SQLite4JavaManager}
								File metadata = getLocalMetadataFile(tablespace, partition, version);
								ThriftReader reader = new ThriftReader(metadata);
								PartitionMetadata partitionMetadata = (PartitionMetadata) reader
								    .read(new PartitionMetadata());
								reader.close();
								SQLite4JavaManager manager = new SQLite4JavaManager(dbFolder + "/" + file,
								    partitionMetadata.getInitStatements());
								manager.setTimeoutThread(timeoutThread);
								dbPoolInCache = new Element(dbKey, manager);
								dbCache.put(dbPoolInCache);
								break;
							}
						}
					}
				}
				if(dbPoolInCache != null) {
					// Query the {@link SQLite4JavaManager} and return
					String result = ((SQLite4JavaManager) dbPoolInCache.getObjectValue()).query(query,
					    maxResultsPerQuery);
					long time = performanceTool.endQuery();
					log.info("serving query [" + tablespace + "]"  + " [" + version + "] [" + partition + "] [" + query + "] time [" + time + "] OK.");
					double prob = performanceTool.getHistogram().getLeftAccumulatedProbability(time);
					if(prob > 0.95) {
						// slow query!
						log.warn("[SLOW QUERY] Query time over 95 percentil: [" + query + "] time [" + time + "]");
					}
					if(time > absoluteSlowQueryLimit) {
						// slow query!
						log.warn("[SLOW QUERY] Query time over absolute slow query time (" + absoluteSlowQueryLimit + ") : [" + query + "] time [" + time + "]");						
					}
					return result;
				} else {
					throw new DNodeException(
					    EXCEPTION_ORDINARY,
					    "Deployed folder doesn't contain a .db file - This shouldn't happen. This means there is a bug or inconsistency in the deploy process.");
				}
			} catch(Throwable e) {
				unexpectedException(e);
				throw new DNodeException(EXCEPTION_UNEXPECTED, e.getMessage());
			}
		} catch(DNodeException e) {
			log.info("serving query [" + tablespace + "]"  + " [" + version + "] [" + partition + "] [" + query + "] FAILED [" + e.getMsg() + "]");
			failedQueries.incrementAndGet();
			throw e;
		}
	}

	private void abortDeploy(long version, String errorMessage) {
		ConcurrentMap<String, String> panel = coord.getDeployErrorPanel(version);
		panel.put(whoAmI(), errorMessage);
		deployInProgress.decrementAndGet();
	}

	/**
	 * Thrift RPC method -> Given a list of {@link DeployAction}s and a version identifying the deployment perform an
	 * asynchronous deploy.
	 */
	@Override
	public String deploy(final List<DeployAction> deployActions, final long version) throws DNodeException {
		try {
			synchronized(deployLock) {
				/*
				 * Here we instantiate a Thread for waiting for the deploy so that we are able to implement deploy timeout... If
				 * the deploy takes too much then we cancel it. We achieve this by using Java asynchronous Future objects.
				 */
				Thread deployWait = new Thread() {
					public void run() {
						Future<?> future = deployThread.submit(new Runnable() {
							// This code is executed by the solely deploy thread, not the one who waits
							@Override
							public void run() {
								try {
									deployInProgress.incrementAndGet();
									lastDeployTimedout.set(false);
									log.info("Starting deploy actions [" + deployActions + "]");
									long start = System.currentTimeMillis();
									for(DeployAction action : deployActions) {
										// 1- Store metadata
										File metadataFile = getLocalMetadataFile(action.getTablespace(),
										    action.getPartition(), version);
										if(!metadataFile.getParentFile().exists()) {
											metadataFile.getParentFile().mkdirs();
										}
										ThriftWriter writer = new ThriftWriter(metadataFile);
										writer.write(action.getMetadata());
										writer.close();
										// 2- Call the fetcher for fetching
										File fetchedContent = fetcher.fetch(action.getDataURI());
										// If we reach this point then the fetch has been OK
										File dbFolder = getLocalStorageFolder(action.getTablespace(), action.getPartition(),
										    version);
										if(dbFolder.exists()) { // If the new folder where we want to deploy already exists means it is
											                      // somehow
											                      // stalled from a previous failed deploy - it is ok to delete it
											FileUtils.deleteDirectory(dbFolder);
										}
										// 4- Perform a "mv" for finally making the data available
										FileUtils.moveDirectory(fetchedContent, dbFolder);
									}

									// Publish new DNodeInfo in distributed registry.
									// This makes QNodes notice that a new version is available...
									// PartitionMap and ReplicationMap will be built incrementally as DNodes finish.
									dnodesRegistry.changeInfo(new DNodeInfo(config));

									// Decrement the countdown latch. On 0, deployer knows that the deploy
									// finished.
									ICountDownLatch countdown = coord.getCountDownLatchForDeploy(version);
									countdown.countDown();

									long end = System.currentTimeMillis();
									log.info("Local deploy actions [" + deployActions + "] successfully finished in "
									    + (end - start) + " ms.");
									deployInProgress.decrementAndGet();
								} catch(Throwable t) {
									// In order to avoid stale deployments, we flag this deploy to be aborted
									log.warn("Error deploying [" + deployActions + "] barrier + [" + version + "]", t);
									abortDeploy(version, ExceptionUtils.getStackTrace(t));
								}
							}
						});
						try {
							// This line makes the wait thread wait for the deploy as long as the configuration tells
							// If the timeout passes a TimeoutException is thrown
							future.get(config.getInt(DNodeProperties.DEPLOY_TIMEOUT_SECONDS), TimeUnit.SECONDS);
						} catch(InterruptedException e) {
							log.warn("Interrupted exception waiting for local deploy to finish - killing deployment",
							    e);
							abortDeploy(version, ExceptionUtils.getStackTrace(e));
							future.cancel(true);
						} catch(ExecutionException e) {
							log.warn("Execution exception waiting for local deploy to finish - killing deployment.", e);
							abortDeploy(version, ExceptionUtils.getStackTrace(e));
						} catch(TimeoutException e) {
							log.warn("Timeout waiting for local deploy to finish - killing deployment.", e);
							abortDeploy(version,
							    "Timeout reached - " + config.getInt(DNodeProperties.DEPLOY_TIMEOUT_SECONDS)
							        + " seconds");
							future.cancel(true);
							lastDeployTimedout.set(true);
						}
					}
				};
				deployWait.start();
			}
			// Everything is asynchronous so this is quickly reached - it just means the process has started
			return JSONSerDe.ser(new DNodeStatusResponse("Ok. Deploy initiated"));
		} catch(Throwable t) {
			unexpectedException(t);
			throw new DNodeException(EXCEPTION_UNEXPECTED, t.getMessage());
		}
	}

	/**
	 * Thrift RPC method -> Given a list of {@link RollbackAction}s, perform a synchronous rollback
	 */
	@Override
	public String rollback(List<RollbackAction> rollbackActions, String ignoreMe) throws DNodeException {
		// The DNode doesn't need to do anything special for rolling back a version.
		// It can serve any version that is stored locally.
		try {
			return JSONSerDe.ser(new DNodeStatusResponse("Ok. Rollback order received."));
		} catch(JSONSerDeException e) {
			unexpectedException(e);
			throw new DNodeException(EXCEPTION_UNEXPECTED, e.getMessage());
		}
	}

	/*
	 * Any unexpected exception must be redirected to this method. In this way we can monitor it using state variables.
	 * The state variables will then be passed onto the appropriated bean in the status() RPC call.
	 */
	private void unexpectedException(Throwable t) {
		t.printStackTrace();
		log.error("Unexpected Exception", t);
		lastException = t.getMessage();
		lastExceptionTime = System.currentTimeMillis();
	}

	/**
	 * Returns an {@link com.splout.db.dnode.beans.DNodeSystemStatus} filled with the appropriated data.
	 */
	@Override
	public String status() throws DNodeException {
		try {
			DNodeSystemStatus status = new DNodeSystemStatus();
			if(lastException == null) {
				status.setSystemStatus("UP");
				status.setLastExceptionTime(-1);
			} else {
				status.setSystemStatus("Last exception: " + lastException);
				status.setLastExceptionTime(lastExceptionTime);
			}
			status.setUpSince(upSince);
			status.setFailedQueries(failedQueries.get());
			status.setnQueries(performanceTool.getNQueries());
			status.setAverage(performanceTool.getAverage());
			status.setSlowQueries(performanceTool.getHistogram().getRigthAccumulatedProbability(1000));
			status.setDeployInProgress(deployInProgress.get() > 0);
			File folder = new File(config.getString(DNodeProperties.DATA_FOLDER));
			status.setFreeSpaceInDisk(FileSystemUtils.freeSpaceKb());
			if(folder.exists()) {
				status.setOccupiedSpaceInDisk(FileUtils.sizeOfDirectory(folder));
				Collection<File> files = FileUtils.listFilesAndDirs(folder, TrueFileFilter.INSTANCE,
				    TrueFileFilter.INSTANCE);
				status.setFiles(new ArrayList<String>(Lists.transform(Lists.newArrayList(files),
				    new Function<File, String>() {
					    @Override
					    public String apply(File file) {
						    return file.getAbsolutePath() + " (" + FileUtils.sizeOf(file) + " bytes)";
					    }
				    })));
				Collections.sort(status.getFiles());
			} else {
				status.setOccupiedSpaceInDisk(0);
				status.setFiles(new ArrayList<String>());
			}
			return JSONSerDe.ser(status);
		} catch(Throwable t) {
			unexpectedException(t);
			throw new DNodeException(EXCEPTION_UNEXPECTED, t.getMessage());
		}
	}

	protected File getLocalStorageFolder(String tablespace, int partition, long version) {
		return getLocalStorageFolder(config, tablespace, partition, version);
	}

	/**
	 * Returns the folder where the DNode that uses the provided Configuration will store the binary data for this
	 * tablespace, version and partition.
	 */
	public static File getLocalStorageFolder(SploutConfiguration config, String tablespace, int partition,
	    long version) {
		String dataFolder = config.getString(DNodeProperties.DATA_FOLDER);
		return new File(dataFolder + "/" + tablespace + "/" + version + "/" + partition);
	}

	protected File getLocalMetadataFile(String tablespace, int partition, long version) {
		return getLocalMetadataFile(config, tablespace, partition, version);
	}

	/**
	 * Returns the file where the DNode that uses the provided Configuration will store the metadata for this tablespace,
	 * version and partition.
	 */
	public static File getLocalMetadataFile(SploutConfiguration config, String tablespace, int partition,
	    long version) {
		String dataFolder = config.getString(DNodeProperties.DATA_FOLDER);
		return new File(dataFolder + "/" + tablespace + "/" + version + "/" + partition + ".meta");
	}

	public boolean isDeployInProgress() {
		return deployInProgress.get() > 0;
	}

	/**
	 * Properly dispose this DNode.
	 */
	public void stop() throws Exception {
		dbCache.dispose();
		deployThread.shutdownNow();
		timeoutThread.interrupt();
		hz.getLifecycleService().shutdown();
	}

	@Override
	public String abortDeploy(long version) throws DNodeException {
		// For simplicity, current implementation cancels all queued deploys.
		// There can only be one deploy being handled at a time, but multiple may have been queued.
		try {
			if(isDeployInProgress()) {
				synchronized(deployLock) { // No new deploys to be handled until we cancel the current one
					// Note that it is not always guaranteed that threads will be properly shutdown...
					deployThread.shutdownNow();
					while(!deployThread.isTerminated()) {
						try {
							Thread.sleep(100);
						} catch(InterruptedException e) {
							log.error("Deploy Thread interrupted - continuing anyway.", e);
						}
					}
					deployThread = Executors.newFixedThreadPool(1);
				}
				return JSONSerDe.ser(new DNodeStatusResponse("Ok. Deploy cancelled."));
			} else {
				return JSONSerDe.ser(new DNodeStatusResponse("No deploy in progress."));
			}
		} catch(JSONSerDeException e) {
			unexpectedException(e);
			throw new DNodeException(EXCEPTION_UNEXPECTED, e.getMessage());
		}
	}

	@Override
	public String deleteOldVersions(List<com.splout.db.thrift.TablespaceVersion> versions)
	    throws DNodeException {
		for(com.splout.db.thrift.TablespaceVersion version : versions) {
			log.info("Going to remove " + version + " as I have been told to do so.");
			try {
				deleteLocalVersion(version);
			} catch(IOException e) {
				unexpectedException(e);
				throw new DNodeException(EXCEPTION_UNEXPECTED, e.getMessage());
			}
		}
		try {
			// Publish new DNodeInfo in distributed registry.
			// This makes QNodes notice that a new version is available...
			// PartitionMap and ReplicationMap will be built incrementally as DNodes finish.
			dnodesRegistry.changeInfo(new DNodeInfo(config));
			return JSONSerDe.ser(new DNodeStatusResponse("Ok. Delete old versions executed."));
		} catch(JSONSerDeException e) {
			unexpectedException(e);
			throw new DNodeException(EXCEPTION_UNEXPECTED, e.getMessage());
		}
	}

	// ----------------- TEST API ----------------- //

	private AtomicBoolean shutDownByTestAPI = new AtomicBoolean(false);

	/*
	 * This method is called by unit / integration tests in order to simulate failures and recoveries in DNodes and such.
	 */
	@Override
	public String testCommand(String commandStr) throws DNodeException {
		if(!config.getBoolean(DNodeProperties.HANDLE_TEST_COMMANDS)) {
			throw new DNodeException(EXCEPTION_ORDINARY, "Can't handle test commands as "
			    + DNodeProperties.HANDLE_TEST_COMMANDS + " is not set to true.");
		}
		TestCommands command = TestCommands.valueOf(commandStr);
		if(command == null) {
			throw new DNodeException(EXCEPTION_ORDINARY, "Unknown test command: " + commandStr);
		}
		if(command.equals(TestCommands.SHUTDOWN)) {
			// on-demand shutdown
			// This is a "soft-shutdown" so we can recover from it.
			// It is designed for unit and integration testing.
			shutDownByTestAPI.set(true);
			dnodesRegistry.unregister();
			hz.getLifecycleService().shutdown();
		} else if(command.equals(TestCommands.RESTART)) {
			// on-demand restart
			// This is a "soft-restart" after a "soft-shutdown".
			// It is designed for unit and integration testing.
			shutDownByTestAPI.set(false);
			try {
				hz = Hazelcast.newHazelcastInstance(HazelcastConfigBuilder.build(config));
				coord = new CoordinationStructures(hz);
				giveGreenLigth();
			} catch(HazelcastConfigBuilderException e) {
				log.error("Error while trying to connect to Hazelcast", e);
				throw new DNodeException(EXCEPTION_UNEXPECTED, e.getMessage());
			}
		}
		try {
			return JSONSerDe.ser(new DNodeStatusResponse("Ok. Test command " + commandStr
			    + " received properly."));
		} catch(JSONSerDeException e) {
			unexpectedException(e);
			throw new DNodeException(EXCEPTION_UNEXPECTED, e.getMessage());
		}
	}
}
