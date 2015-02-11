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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.transport.TTransportException;

import com.google.common.base.Joiner;
import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;
import com.splout.db.common.SploutConfiguration;
import com.splout.db.common.Tablespace;
import com.splout.db.dnode.DNodeClient;
import com.splout.db.hazelcast.CoordinationStructures;
import com.splout.db.hazelcast.DNodeInfo;
import com.splout.db.hazelcast.TablespaceVersion;
import com.splout.db.qnode.ReplicaBalancer.BalanceAction;
import com.splout.db.thrift.DNodeService;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;

/**
 * This class contains the basic context of {@link QNodeHandler}. This context involves in-memory information of the
 * system such as: list of alive DNodes, list of tablespaces, versions and so forth. In addition, this class also
 * maintains a pool of connections to the DNodes. This class is shared among all different {@link QNodeHandlerModule}
 * such as {@link Deployer} so that each specialized module can have access to the context.
 */
public class QNodeHandlerContext {

  protected final static Log log = LogFactory.getLog(QNodeHandlerContext.class);

  // This map indicates which is the current version being served. It has to be updated atomically.
  private final Map<String, Long> currentVersionsMap = new ConcurrentHashMap<String, Long>();
  private String qNodeAddress;
  // The SploutConfiguration
  private SploutConfiguration config;
  // The coordination structures that use Hazelcast underneath
  private CoordinationStructures coordinationStructures;
  private TablespaceMemoryState tablespaceState = new TablespaceMemoryState();
  private ReplicaBalancer replicaBalancer;

  // This flag is set to "false" after WARMING_TIME seconds (qnode.warming.time)
  // Some actions will only be taken after warming time, just in case some nodes still didn't join the cluster.
  private final AtomicBoolean isWarming = new AtomicBoolean(true);

  // The per-DNode Thrift client pools
  private ConcurrentMap<String, BlockingQueue<DNodeService.Client>> thriftClientCache = new ConcurrentHashMap<String, BlockingQueue<DNodeService.Client>>();
  private ReentrantLock thriftClientCacheLock = new ReentrantLock();

  private final int thriftClientPoolSize;
  private final long dnodePoolTimeoutMillis;

  public QNodeHandlerContext(SploutConfiguration config, CoordinationStructures coordinationStructures) {
    this.config = config;
    this.coordinationStructures = coordinationStructures;
    this.thriftClientPoolSize = config.getInt(QNodeProperties.DNODE_POOL_SIZE);
    this.dnodePoolTimeoutMillis = config.getLong(QNodeProperties.QNODE_DNODE_POOL_TAKE_TIMEOUT);
    this.replicaBalancer = new ReplicaBalancer(this);
    initMetrics();
  }

  public static enum DNodeEvent {
    LEAVE, ENTRY, UPDATE
  }

  private void initMetrics() {
    Metrics.newGauge(QNodeHandlerContext.class, "thrift-total-connections-iddle", new Gauge<Integer>() {
      @Override
      public Integer value() {
        int count = 0;
        for (Entry<String, BlockingQueue<DNodeService.Client>> queue : thriftClientCache.entrySet()) {
          count += queue.getValue().size();
        }
        return count;
      }
    });
    Metrics.newGauge(QNodeHandlerContext.class, "thrift-total-connections-being-used", new Gauge<Integer>() {
      @Override
      public Integer value() {
        int queues = 0;
        int count = 0;
        for (Entry<String, BlockingQueue<DNodeService.Client>> queue : thriftClientCache.entrySet()) {
          queues++;
          count += queue.getValue().size();
        }
        return (QNodeHandlerContext.this.thriftClientPoolSize * queues) - count;
      }
    });
    Metrics.newGauge(QNodeHandlerContext.class, "thrift-pools", new Gauge<String>() {
      @Override
      public String value() {
        ArrayList<String> fullPools = new ArrayList<String>();
        for (Entry<String, BlockingQueue<DNodeService.Client>> queue : thriftClientCache.entrySet()) {
          int idle = queue.getValue().size();
          int size = QNodeHandlerContext.this.thriftClientPoolSize;
          fullPools.add("Pool: " + queue.getKey() + " (" + (size - idle) + " of " + size + ") being used");
        }
        return Joiner.on(", ").join(fullPools);
      }
    });
    Metrics.newGauge(QNodeHandlerContext.class, "thrift-pools", new Gauge<Integer>() {
      @Override
      public Integer value() {
        return thriftClientCache.size();
      }
    });
    Metrics.newGauge(QNodeHandlerContext.class, "thrift-total-configured-connections", new Gauge<Integer>() {
      @Override
      public Integer value() {
        return (QNodeHandlerContext.this.thriftClientPoolSize * thriftClientCache.size());
      }
    });
  }

  @SuppressWarnings("serial")
  public final static class TablespaceVersionInfoException extends Exception {

    public TablespaceVersionInfoException(String msg) {
      super(msg);
    }
  }

  /**
   * Get the list of possible actions to take for balancing the cluster in case of under-replicated partitions.
   */
  public List<BalanceAction> getBalanceActions() {
    // we have this in this class to be able to use this lock (the same that recreats the in-memory TablespaceVersion map)
    synchronized (tablespaceState) {
      return replicaBalancer.scanPartitions();
    }
  }

  /**
   * Get the list of DNodes
   */
  public List<String> getDNodeList() {
    List<String> dNodeList = new ArrayList<String>();
    for (DNodeInfo dnode : getCoordinationStructures().getDNodes().values()) {
      dNodeList.add(dnode.getAddress());
    }
    return dNodeList;
  }

  

  /**
   * This method can be called to initialize a pool of connections to a dnode. This method may be called from multiple
   * threads so it should be safe to call it concurrently.
   */
  public void initializeThriftClientCacheFor(String dnode) throws TTransportException,
      InterruptedException {
    // this lock is on the whole cache but we would actually be interested in a per-DNode lock...
    // there's only one lock for simplicity.
    thriftClientCacheLock.lock();
    try {
      // initialize queue for this DNode
      BlockingQueue<DNodeService.Client> dnodeQueue = thriftClientCache.get(dnode);
      if (dnodeQueue == null) {
        // this assures that the per-DNode queue is only created once and then reused.
        dnodeQueue = new LinkedBlockingDeque<DNodeService.Client>(thriftClientPoolSize);
      }
      if (dnodeQueue.isEmpty()) {
        try {
          for (int i = dnodeQueue.size(); i < thriftClientPoolSize; i++) {
            dnodeQueue.put(DNodeClient.get(dnode));
          }
          // we only put the queue if all connections have been populated
          thriftClientCache.put(dnode, dnodeQueue);
        } catch (TTransportException e) {
          log.error("Error while trying to populate queue for " + dnode
              + ", will discard created connections.", e);
          while (!dnodeQueue.isEmpty()) {
            dnodeQueue.poll().getOutputProtocol().getTransport().close();
          }
          throw e;
        }
      } else {
        // it should be safe to call this method from different places concurrently
        // so we contemplate the case where another Thread already populated the queue
        // and only populate it if it's really empty.
        log.warn(Thread.currentThread().getName() + " : queue for [" + dnode
            + "] is not empty - it was populated before.");
      }
    } finally {
      thriftClientCacheLock.unlock();
    }
  }

  /**
   * This method can be called by {@link QNodeHandler} to cancel the Thrift client cache when a DNode disconnects.
   * Usually this happens when Hazelcast notifies it.
   */
  public void discardThriftClientCacheFor(String dnode) throws InterruptedException {
    thriftClientCacheLock.lock();
    try {
      // discarding all connections to a DNode who leaved
      log.info(Thread.currentThread().getName() + " : trashing queue for [" + dnode + "] as it leaved.");
      BlockingQueue<DNodeService.Client> dnodeQueue = thriftClientCache.get(dnode);
      // release connections until empty
      while (!dnodeQueue.isEmpty()) {
        dnodeQueue.take().getOutputProtocol().getTransport().close();
      }
      thriftClientCache.remove(dnode); // to indicate that the DNode is not present
    } finally {
      thriftClientCacheLock.unlock();
    }
  }

  /**
   * Get the Thrift client for this DNode.
   * <p/>
   * Can throw a TTransportException in the rare case when
   * a new pool is initialized here. In this case, you shouldn't call
   * the method {@link #returnDNodeClientToPool(String, com.splout.db.thrift.DNodeService.Client, boolean)}
   * to return the connection.
   * <p/>
   * This method never returns null.
   *
   * @throws java.lang.InterruptedException             if somebody interrupts the thread meanwhile the method is waiting in the pool
   * @throws com.splout.db.qnode.PoolCreationException  if there is failure when a new pool is created.
   * @throws com.splout.db.qnode.DNodePoolFullException if the pool for the given dnode is empty and the timeout
   *                                                    for waiting for a connection is reached.
   */
  public DNodeService.Client getDNodeClientFromPool(String dnode) throws InterruptedException, PoolCreationException,
      DNodePoolFullException {
    BlockingQueue<DNodeService.Client> dnodeQueue = thriftClientCache.get(dnode);
    if (dnodeQueue == null) {
      // This shouldn't happen in real life because it is initialized by the QNode, but it is useful for unit
      // testing.
      // Under some rare race conditions the pool may be required before the QNode creates it, but this method
      // assures that the queue will only be created once and, if it's not possible to create it, an exception
      // will be thrown and nothing bad will happen.
      try {
        initializeThriftClientCacheFor(dnode);
        dnodeQueue = thriftClientCache.get(dnode);
      } catch (TTransportException e) {
        throw new PoolCreationException(e);
      }
    }

    DNodeService.Client client = dnodeQueue.poll(dnodePoolTimeoutMillis, TimeUnit.MILLISECONDS);
    // Timeout waiting for poll
    if (client == null) {
      throw new DNodePoolFullException("Pool for dnode[" + dnode + "] is full and timeout of [" + dnodePoolTimeoutMillis
          + "] reached when waiting for a connection.");
    }
    return client;
  }

  /**
   * Return a Thrift client to the pool. This method is a bit tricky since we may want to return a connection when a
   * DNode already disconnected. Also, if the QNode is closing, we don't want to leave opened sockets around. To do it
   * safely, we check whether 1) we are closing / cleaning the QNode or 2) the DNode has disconnected.
   * <p/>
   * The given client never can be null.
   */
  public void returnDNodeClientToPool(String dnode, DNodeService.Client client, boolean renew) {
    if (closing.get()) { // don't return to the pool if the system is already closing! we must close everything!
      if (client != null) {
        client.getOutputProtocol().getTransport().close();
      }
      return;
    }
    BlockingQueue<DNodeService.Client> dnodeQueue = thriftClientCache.get(dnode);
    if (dnodeQueue == null) {
      // dnode is not connected, so we exit.
      if (client != null) {
        client.getOutputProtocol().getTransport().close();
      }
      return;
    }
    if (renew) { // we have to try to renew the connection
      try {
        DNodeService.Client newClient = DNodeClient.get(dnode);
        if (client != null) {
          client.getOutputProtocol().getTransport().close();
          client = newClient;
        }
      } catch (TTransportException e) {
        // Was not possible to renew connection. We'll keep the broken one.
        log.warn(
            "TTransportException while renewing client to dnode[" + dnode + "]. Broken client is returned to the pool as is to continue.");
      }
    }
    try {
      dnodeQueue.add(client);
    } catch (IllegalStateException e) {
      client.getOutputProtocol().getTransport().close();
      log.error("Trying to return a connection for dnode ["
          + dnode
          + "] but the pool already has the maximum number of connections. This is likely a software bug!.");
    }

    // one last check to avoid not closing every socket.
    // here we avoid leaking a socket in case a close has happened in parallel or a DNode disconnected right in the
    // middle
    if (closing.get() || thriftClientCache.get(dnode) == null) {
      if (client != null) {
        client.getOutputProtocol().getTransport().close();
      }
    }
  }

  /**
   * Rotates the versions (deletes versions that are old or useless). To be executed at startup and after a deployment.
   */
  public List<com.splout.db.thrift.TablespaceVersion> synchronizeTablespaceVersions()
      throws InterruptedException {
    log.info("Starting to look for old tablespace versions to remove...");

    int maxVersionsPerTablespace = config.getInt(QNodeProperties.VERSIONS_PER_TABLESPACE);

    // Will contain the list of versions per each tablespace, sorted by creation date descendant
    TreeMultimap<String, Tablespace> tablespaces = TreeMultimap.create(Ordering.natural(),
        new Comparator<Tablespace>() {
          @Override
          public int compare(Tablespace tb1, Tablespace tb2) {
            // reverse ordering. Older dates appears LAST. If same date, then version is compared.
            int comp = -((Long) tb1.getCreationDate()).compareTo(tb2.getCreationDate());
            if (comp == 0) {
              return -((Long) tb1.getVersion()).compareTo(tb2.getVersion());
            } else {
              return comp;
            }
          }
        });

    Map<TablespaceVersion, Tablespace> myTablespaces = getTablespaceVersionsMap();

    // We build a in memory version of tablespaces for analyzing it
    // and prune old ones.
    for (Entry<TablespaceVersion, Tablespace> entry : myTablespaces.entrySet()) {
      tablespaces.put(entry.getKey().getTablespace(), entry.getValue());
    }
    log.info("Analyzing " + tablespaces.keySet().size() + " tablespaces with a total of " + tablespaces.size() + " versions...");

    // We will remove only versions older than the one being served
    Map<String, Long> hzVersionsBeingServed = coordinationStructures.getCopyVersionsBeingServed();
    if (hzVersionsBeingServed == null) {
      log.info("... No versions yet being served.");
      return null; // nothing to do yet
    }
    log.info("Number of versions being served: " + hzVersionsBeingServed.size());

    List<com.splout.db.thrift.TablespaceVersion> tablespacesToRemove = new ArrayList<com.splout.db.thrift.TablespaceVersion>();

    for (Entry<String, Long> entry : hzVersionsBeingServed.entrySet()) {
      String tablespace = entry.getKey();
      Long versionBeingServed = entry.getValue();
      // Tablespaces are sorted by creation date desc.
      SortedSet<Tablespace> allVersions = tablespaces.get(tablespace);
      Iterator<Tablespace> it = allVersions.iterator();
      boolean foundVersionBeingServed = false;
      int countVersionsAfter = 0;
      while (it.hasNext()) {
        Tablespace tb = it.next();
        if (versionBeingServed.equals(tb.getVersion())) {
          foundVersionBeingServed = true;
        } else {
          if (foundVersionBeingServed) {
            countVersionsAfter++;
            if (countVersionsAfter >= maxVersionsPerTablespace) {
              // This is the case where we remove the version
              // 1 - This tablespace has a version being served
              // 2 - This version is older than the current tablespace being served
              // 3 - We are already keeping maxVersionsPerTablespace versions
              tablespacesToRemove.add(new com.splout.db.thrift.TablespaceVersion(tablespace, tb
                  .getVersion()));
              log.info("Tablespace [" + tablespace + "] Version [" + tb.getVersion() + "] "
                  + "created at [" + new Date(tb.getCreationDate())
                  + "] REMOVED. We already keep younger versions.");
            } else {
              log.info("Tablespace [" + tablespace + "] Version [" + tb.getVersion() + "] "
                  + "created at [" + new Date(tb.getCreationDate())
                  + "] KEPT.");
            }
          } else {
            log.info("Tablespace [" + tablespace + "] Version [" + tb.getVersion() + "] "
                + "created at [" + new Date(tb.getCreationDate()) + "] either younger than served one or without version being served. Keeping.");
          }
        }
      }

      if (!foundVersionBeingServed) {
        log.info("Tablespace [" + tablespace
            + "] without any version being served. Please, have a look, and remove them if not used");
      }

      if (tablespacesToRemove.size() > 0) {
        log.info("Sending [" + tablespacesToRemove + "] to all alive DNodes.");
        for (DNodeInfo dnode : coordinationStructures.getDNodes().values()) {
          DNodeService.Client client = null;
          boolean renew = false;
          try {
            client = getDNodeClientFromPool(dnode.getAddress());
            client.deleteOldVersions(tablespacesToRemove);
          } catch (TTransportException e) {
            renew = true;
            log.warn("Failed sending delete TablespaceVersions order to (" + dnode
                + "). Not critical as they will be removed after other deployments.", e);
          } catch (Exception e) {
            log.warn("Failed sending delete TablespaceVersions order to (" + dnode
                + "). Not critical as they will be removed after other deployments.", e);
          } finally {
            if (client != null) {
              returnDNodeClientToPool(dnode.getAddress(), client, renew);
            }
          }
        }
      }
      log.info("... done looking for old tablespace versions to remove...");
    }

    return tablespacesToRemove; // Return for unit test
  }

  private AtomicBoolean closing = new AtomicBoolean(false);

  public void close() {
    closing.set(true); // will indicate other parts of this code that things have to be closed!
    for (Map.Entry<String, BlockingQueue<DNodeService.Client>> entry : thriftClientCache.entrySet()) {
      while (entry.getValue().size() > 0) {
        try {
          entry.getValue().take().getOutputProtocol().getTransport().close();
        } catch (InterruptedException e) {
          log.error("Interrupted!", e);
        }
      }
    }
  }

  public void maybeBalance() {
    // do this only after warming
    if (!isWarming.get() && config.getBoolean(QNodeProperties.REPLICA_BALANCE_ENABLE)) {
      // check if we could balance some partitions
      List<ReplicaBalancer.BalanceAction> balanceActions = getBalanceActions();
      // we will only re-balance versions being served
      // otherwise strange things may happen: to re-balance a version in the middle of its deployment...
      Map<String, Long> versionsBeingServed = coordinationStructures.getCopyVersionsBeingServed();
      for (ReplicaBalancer.BalanceAction action : balanceActions) {
        if (versionsBeingServed != null && versionsBeingServed.get(action.getTablespace()) != null
            && versionsBeingServed.get(action.getTablespace()) == action.getVersion()) {
          // put if absent + TTL
          coordinationStructures.getDNodeReplicaBalanceActionsSet().putIfAbsent(action, "",
              config.getLong(QNodeProperties.BALANCE_ACTIONS_TTL), TimeUnit.SECONDS);
        }
      }
    }
  }

  // ---- Getters ---- //

  public Map<String, Long> getCurrentVersionsMap() {
    return currentVersionsMap;
  }

  public Map<TablespaceVersion, Tablespace> getTablespaceVersionsMap() {
    return tablespaceState.getTablespaceVersionsMap();
  }

  public CoordinationStructures getCoordinationStructures() {
    return coordinationStructures;
  }

  public TablespaceMemoryState getTablespaceState() {
    return tablespaceState;
  }
  
  public SploutConfiguration getConfig() {
    return config;
  }

  public ConcurrentMap<String, BlockingQueue<DNodeService.Client>> getThriftClientCache() {
    return thriftClientCache;
  }

  public AtomicBoolean getIsWarming() {
    return isWarming;
  }

  public String getQNodeAddress() { return qNodeAddress; }

  public void setQNodeAddress(String QNodeAddress) {
    this.qNodeAddress = QNodeAddress;
  }
}