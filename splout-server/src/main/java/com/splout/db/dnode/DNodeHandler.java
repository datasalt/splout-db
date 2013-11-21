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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICountDownLatch;
import com.splout.db.benchmark.PerformanceTool;
import com.splout.db.common.JSONSerDe;
import com.splout.db.common.JSONSerDe.JSONSerDeException;
import com.splout.db.common.SploutConfiguration;
import com.splout.db.common.ThriftReader;
import com.splout.db.common.ThriftWriter;
import com.splout.db.dnode.beans.BalanceFileReceivingProgress;
import com.splout.db.dnode.beans.DNodeStatusResponse;
import com.splout.db.dnode.beans.DNodeSystemStatus;
import com.splout.db.engine.EngineManager;
import com.splout.db.engine.ManagerFactory;
import com.splout.db.hazelcast.CoordinationStructures;
import com.splout.db.hazelcast.DNodeInfo;
import com.splout.db.hazelcast.DistributedRegistry;
import com.splout.db.hazelcast.HazelcastConfigBuilder;
import com.splout.db.hazelcast.HazelcastConfigBuilder.HazelcastConfigBuilderException;
import com.splout.db.hazelcast.HazelcastProperties;
import com.splout.db.qnode.ReplicaBalancer;
import com.splout.db.qnode.ReplicaBalancer.BalanceAction;
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

	protected SploutConfiguration config;
	private HazelcastInstance hz;
	private DistributedRegistry dnodesRegistry;
	private CoordinationStructures coord;
	private HttpFileExchanger httpExchanger;

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
	private long slowQueries = 0;

	// This map will hold all the current balance file transactions being done
	private ConcurrentHashMap<String, BalanceFileReceivingProgress> balanceActionsStateMap = new ConcurrentHashMap<String, BalanceFileReceivingProgress>();

	// The factory we will use to instantiate managers for each partition associated with an {@link Engine}
	private ManagerFactory factory;

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

	public String httpExchangerAddress() {
		return "http://" + config.getString(DNodeProperties.HOST) + ":"
		    + config.getInt(HttpFileExchangerProperties.HTTP_PORT);
	}

	/**
	 * This inner class will listen for additions to the balance actions map, so that if a balance action has to be taken
	 * and this DNode is the one who has the send the file, it will start doing so.
	 */
	private class BalanceActionItemListener implements
	    EntryListener<ReplicaBalancer.BalanceAction, String> {

		@Override
		public void entryAdded(EntryEvent<BalanceAction, String> event) {
			BalanceAction action = event.getKey();
			if(action.getOriginNode().equals(whoAmI())) {
				// I must do a balance action!
				File toSend = new File(getLocalStorageFolder(action.getTablespace(), action.getPartition(),
				    action.getVersion()), action.getPartition() + ".db");
				File metadataFile = getLocalMetadataFile(action.getTablespace(), action.getPartition(),
				    action.getVersion());
				// send both the .db and the .meta file -> when the other part has both files it will move them atomically...
				httpExchanger.send(action.getTablespace(), action.getPartition(), action.getVersion(), toSend,
				    action.getFinalNode(), false);
				httpExchanger.send(action.getTablespace(), action.getPartition(), action.getVersion(),
				    metadataFile, action.getFinalNode(), false);
			}
		}

		@Override
		public void entryRemoved(EntryEvent<BalanceAction, String> event) {
			// usually we won't care - but the final DNode might have pro-actively removed this action
		}

		@Override
		public void entryUpdated(EntryEvent<BalanceAction, String> event) {
		}

		@Override
		public void entryEvicted(EntryEvent<BalanceAction, String> event) {
		}
	}

	/**
	 * This inner class will perform the business logic associated with receiving files: what to do on failures, bad CRC,
	 * file received OK...
	 */
	private class FileReceiverCallback implements HttpFileExchanger.ReceiveFileCallback {

		@Override
		public void onProgress(String tablespace, Integer partition, Long version, File file,
		    long totalSize, long sizeDownloaded) {

			if(file.getName().endsWith(".db")) {
				getProgressFromLocalPanel(tablespace, partition, version).progressBinaryFile(totalSize,
				    sizeDownloaded);
			}
		}

		@Override
		public void onFileReceived(String tablespace, Integer partition, Long version, File file) {
			BalanceFileReceivingProgress progress = getProgressFromLocalPanel(tablespace, partition, version);
			if(file.getName().endsWith(".meta")) {
				progress.metaFileReceived(file);
			} else if(file.getName().endsWith(".db")) {
				progress.binaryFileReceived(file);
			}

			// this can be reached simultaneously by 2 different threads so we must synchronized it
			// (thread that downloaded the .meta file and thread that downloaded the .db file)
			synchronized(FileReceiverCallback.this) {
				if(progress.isReceivedMetaFile() && progress.isReceivedBinaryFile()) {
					// This assures that the move will only be done once
					if(new File(progress.getMetaFile()).exists() && new File(progress.getBinaryFile()).exists()) {
						// check if we already have the binary & meta -> then move partition
						// and then remove this action from the panel so that it's completed.
						try {
							// we need to remove existing files if they exist
							// they might be stalled from old deployments
							File meta = getLocalMetadataFile(tablespace, partition, version);
							if(meta.exists()) {
								meta.delete();
							}
							FileUtils.moveFile(new File(progress.getMetaFile()), meta);
							File binaryToMove = new File(progress.getBinaryFile());
							File binary = new File(getLocalStorageFolder(tablespace, partition, version),
							    binaryToMove.getName());
							if(binary.exists()) {
								binary.delete();
							}
							FileUtils.moveToDirectory(binaryToMove,
							    getLocalStorageFolder(tablespace, partition, version), true);
							log.info("Balance action successfully completed, received both .db and .meta files ("
							    + tablespace + ", " + partition + ", " + version + ")");
							// Publish new changes to HZ
							dnodesRegistry.changeInfo(new DNodeInfo(config));
						} catch(IOException e) {
							log.error(e);
						} finally {
							removeBalanceActionFromHZPanel(tablespace, partition, version);
						}
					}
				}
			}
		}

		@Override
		public void onBadCRC(String tablespace, Integer partition, Long version, File file) {
			removeBalanceActionFromHZPanel(tablespace, partition, version);
		}

		@Override
		public void onError(Throwable t, String tablespace, Integer partition, Long version, File file) {
			removeBalanceActionFromHZPanel(tablespace, partition, version);
		}

		// --- Helper methods --- //

		/**
		 * Will remove the BalanceAction associated with this file receiving from HZ data structure.
		 */
		private synchronized void removeBalanceActionFromHZPanel(String tablespace, Integer partition,
		    Long version) {
			// first remove the local tracking of this action
			String lookupKey = tablespace + "_" + partition + "_" + version;
			if(balanceActionsStateMap.containsKey(lookupKey)) {
				balanceActionsStateMap.remove(lookupKey);
				// then remove from HZ
				BalanceAction actionToRemove = null;
				for(Map.Entry<BalanceAction, String> actionEntry : coord.getDNodeReplicaBalanceActionsSet()
				    .entrySet()) {
					BalanceAction action = actionEntry.getKey();
					if(action.getTablespace().equals(tablespace) && action.getPartition() == partition
					    && action.getVersion() == version && action.getFinalNode().equals(httpExchanger.address())) {
						actionToRemove = action;
					}
				}
				if(actionToRemove == null) {
					// no need to worry - another thread might have gone into this code already almost simultaneously
				} else {
					coord.getDNodeReplicaBalanceActionsSet().remove(actionToRemove);
					log.info("Removed balance action [" + actionToRemove + "] from HZ panel.");
				}
			}
		}

		/**
		 * Will obtain a bean to fill some progress in a local hashmap or create it and put it otherwise.
		 */
		private synchronized BalanceFileReceivingProgress getProgressFromLocalPanel(String tablespace,
		    Integer partition, Long version) {
			String lookupKey = tablespace + "_" + partition + "_" + version;
			BalanceFileReceivingProgress progress = balanceActionsStateMap.get(lookupKey);
			if(progress == null) {
				progress = new BalanceFileReceivingProgress(tablespace, partition, version);
				balanceActionsStateMap.put(lookupKey, progress);
			}
			return progress;
		}
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
		factory = new ManagerFactory();
		factory.init(config);
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
		// A thread that will listen to file exchanges through HTTP
		httpExchanger = new HttpFileExchanger(config, new FileReceiverCallback());
		httpExchanger.init();
		httpExchanger.start();
		// Connect with the cluster.
		hz = Hazelcast.newHazelcastInstance(HazelcastConfigBuilder.build(config));
		coord = new CoordinationStructures(hz);
		coord.getDNodeReplicaBalanceActionsSet().addEntryListener(new BalanceActionItemListener(), false);
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
			File[] partitions = versionFolder.listFiles();
			if(partitions != null) {
				for(File partition : partitions) {
					if(partition.isDirectory()) {
						// remove references to engine in ECache
						// so that space in disk is immediately available
						String dbKey = version.getTablespace() + "_" + version.getVersion() + "_"
						    + partition.getName();
						synchronized(dbCache) {
							if(dbCache.get(dbKey) != null) {
								dbCache.remove(dbKey);
								log.info("-- Removing references from ECache: " + dbKey);
							}
						}
					}
				}
			}
			FileUtils.deleteDirectory(versionFolder);
			log.info("-- Successfully removed " + versionFolder);
		} else {
			// Could happen, nothing to worry
		}
	}

	/**
	 * This method will be called either before publishing a new tablespace after a deploy or when a query is issued
	 * to a tablespace/version which is not "warmed" (e.g. after Splout restart, or after long inactivity).
	 */
	private void loadManagerInEHCache(String tablespace, long version, int partition, File dbFolder, PartitionMetadata partitionMetadata) throws DNodeException {
		try {
			// Create new EHCache item value with a {@link EngineManager}
			EngineManager manager = factory.getManagerIn(dbFolder, partitionMetadata);
			String dbKey = tablespace + "_" + version + "_" + partition;
			Element dbPoolInCache = new Element(dbKey, manager);
			dbCache.put(dbPoolInCache);
		} catch(Exception e) {
			log.warn(e);
			throw new DNodeException(EXCEPTION_ORDINARY,
			    "Error (" + e.getMessage() + ") instantiating a manager for a data partition");
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
						File metadata = getLocalMetadataFile(tablespace, partition, version);
						ThriftReader reader = new ThriftReader(metadata);
						PartitionMetadata partitionMetadata = (PartitionMetadata) reader
						    .read(new PartitionMetadata());
						reader.close();
						loadManagerInEHCache(tablespace, version, partition, dbFolder, partitionMetadata);
					}
				}
				// Query the {@link SQLite4JavaManager} and return
				String result = ((EngineManager) dbPoolInCache.getObjectValue())
				    .query(query, maxResultsPerQuery);
				long time = performanceTool.endQuery();
				log.info("serving query [" + tablespace + "]" + " [" + version + "] [" + partition + "] ["
				    + query + "] time [" + time + "] OK.");
				// double prob = performanceTool.getHistogram().getLeftAccumulatedProbability(time);
				// if(prob > 0.95) {
				// // slow query!
				// log.warn("[SLOW QUERY] Query time over 95 percentil: [" + query + "] time [" + time + "]");
				// slowQueries++;
				// }
				if(time > absoluteSlowQueryLimit) {
					// slow query!
					log.warn("[SLOW QUERY] Query time over absolute slow query time (" + absoluteSlowQueryLimit
					    + ") : [" + query + "] time [" + time + "]");
					slowQueries++;
				}
				return result;
			} catch(Throwable e) {
				unexpectedException(e);
				throw new DNodeException(EXCEPTION_UNEXPECTED, e.getMessage());
			}
		} catch(DNodeException e) {
			log.info("serving query [" + tablespace + "]" + " [" + version + "] [" + partition + "] [" + query
			    + "] FAILED [" + e.getMsg() + "]");
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
									long totalSize = 0;

									// Ask for the total size of the deployment first.
									for(DeployAction action : deployActions) {
										long plusSize = fetcher.sizeOf(action.getDataURI());
										if(plusSize == Fetcher.SIZE_UNKNOWN) {
											totalSize = Fetcher.SIZE_UNKNOWN;
											break;
										}
										totalSize += plusSize;
									}

									final double totalKnownSize = totalSize / (1024d * 1024d);
									final long startTime = System.currentTimeMillis();
									final AtomicLong bytesSoFar = new AtomicLong(0l);

									Fetcher.Reporter reporter = new Fetcher.Reporter() {
										@Override
										public void progress(long consumed) {
											long now = System.currentTimeMillis();
											double totalSoFar = bytesSoFar.addAndGet(consumed) / (1024d * 1024d);
											double secondsSoFar = (now - startTime) / 1000d;
											double mBytesPerSec = totalSoFar / secondsSoFar;

											String msg = "[" + whoAmI() + " progress/speed report]: Fetched [";
											if(totalSoFar > 999) {
												msg += String.format("%.3f", (totalSoFar / 1024d)) + "] GBs so far ";
											} else {
												msg += String.format("%.3f", totalSoFar) + "] MBs so far ";
											}

											if(totalKnownSize != Fetcher.SIZE_UNKNOWN) {
												msg += "(out of [";
												if(totalKnownSize > 999) {
													msg += String.format("%.3f", (totalKnownSize / 1024d)) + "] GBs) ";
												} else {
													msg += String.format("%.3f", totalKnownSize) + "] MBs) ";
												}
											}
											msg += "- Current deployment speed is [" + String.format("%.3f", mBytesPerSec)
											    + "] MB/s.";
											// Add a report of the estimated remaining time if we can
											if(totalKnownSize != Fetcher.SIZE_UNKNOWN) {
												double missingSize = (totalKnownSize - totalSoFar);
												long remainingSecs = (long) (missingSize / mBytesPerSec);
												String timeRemaining = "";
												if(remainingSecs > 3600) { // hours, minutes and secs
													int hours = (int) (remainingSecs / 3600);
													int restOfSeconds = (int) (remainingSecs % 3600);
													timeRemaining = hours + " hours and " + (int) (restOfSeconds / 60)
													    + " minutes and " + (restOfSeconds % 60) + " seconds";
												} else if(remainingSecs > 60) { // minutes and secs
													timeRemaining = (int) (remainingSecs / 60) + " minutes and "
													    + (remainingSecs % 60) + " seconds";
												} else { // secs
													timeRemaining = remainingSecs + " seconds";
												}
												msg += " Estimated remaining time is [" + timeRemaining + "].";
											}
											log.info(msg);
											coord.logDeploySpeed(version, whoAmI(), msg);
										}
									};

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
										File fetchedContent = fetcher.fetch(action.getDataURI(), reporter);
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
										// 5- Preemptively load the Manager in case initialization is slow
										// Managers might warm up for a while (e.g. loading data into memory)
										loadManagerInEHCache(action.getTablespace(), action.getVersion(), action.getPartition(), dbFolder, action.getMetadata());
									}

									// Publish new DNodeInfo in distributed registry.
									// This makes QNodes notice that a new version is available...
									// PartitionMap and ReplicationMap will be built incrementally as DNodes finish.
									dnodesRegistry.changeInfo(new DNodeInfo(config));

									long end = System.currentTimeMillis();
									log.info("Local deploy actions [" + deployActions + "] successfully finished in "
									    + (end - start) + " ms.");
									deployInProgress.decrementAndGet();
								} catch(Throwable t) {
									// In order to avoid stale deployments, we flag this deploy to be aborted
									log.warn("Error deploying [" + deployActions + "] barrier + [" + version + "]", t);
									abortDeploy(version, ExceptionUtils.getStackTrace(t));
								} finally {
									// Decrement the countdown latch. On 0, deployer knows that the deploy
									// finished.
									ICountDownLatch countdown = coord.getCountDownLatchForDeploy(version);
									countdown.countDown();
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
			status.setSlowQueries(slowQueries);
			status.setDeployInProgress(deployInProgress.get() > 0);
			status.setHttpExchangerAddress(httpExchangerAddress());
			status.setBalanceActionsStateMap(balanceActionsStateMap);
			File folder = new File(config.getString(DNodeProperties.DATA_FOLDER));
			if(folder.exists()) {
				status.setFreeSpaceInDisk(FileSystemUtils.freeSpaceKb(folder.toString()));
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
				status.setFreeSpaceInDisk(FileSystemUtils.freeSpaceKb("."));
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
		return new File(dataFolder + "/"
		    + getLocalStoragePartitionRelativePath(tablespace, partition, version));
	}

	public static String getLocalStoragePartitionRelativePath(String tablespace, int partition,
	    long version) {
		return tablespace + "/" + version + "/" + partition;
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
		return new File(dataFolder + "/" + getLocalMetadataFileRelativePath(tablespace, partition, version));
	}

	public static String getLocalMetadataFileRelativePath(String tablespace, int partition, long version) {
		return tablespace + "/" + version + "/" + partition + ".meta";
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
		factory.close();
		httpExchanger.close();
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
			} catch(Throwable e) {
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
			log.info("Received a shutdown by test API.");
			hz.getLifecycleService().shutdown();
		} else if(command.equals(TestCommands.RESTART)) {
			// on-demand restart
			// This is a "soft-restart" after a "soft-shutdown".
			// It is designed for unit and integration testing.
			shutDownByTestAPI.set(false);
			try {
				hz = Hazelcast.newHazelcastInstance(HazelcastConfigBuilder.build(config));
				coord = new CoordinationStructures(hz);
				log.info("Received a restart by test API.");
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

	// --- Getters mainly for testing --- /

	public CoordinationStructures getCoord() {
		return coord;
	}

	public DistributedRegistry getDnodesRegistry() {
		return dnodesRegistry;
	}
}
