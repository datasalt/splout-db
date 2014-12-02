package com.splout.db.hazelcast;

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

import com.hazelcast.core.*;
import com.splout.db.qnode.Deployer;
import com.splout.db.qnode.ReplicaBalancer;
import com.splout.db.qnode.beans.DeployInfo;
import com.splout.db.qnode.beans.DeployStatus;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class that centralizes the distributed structures used for coordinate the Splout cluster.
 */
public class CoordinationStructures {

  public HazelcastInstance getHz() {
    return hz;
  }

  /*
   * Map<String, Map<String, Long>> that keeps the version that is being served currently for each tablespace It only
   * keeps one entry in the map with the full versions, keyed by KEY_FOR_VERSIONS_BEING_SERVED. This is done that way,
   * instead having one entry per tablespace because of atomicity: Each time one, or severals tablespaces versions have
   * changes, we receive ALL CHANGES all together.
   */
  public static final String VERSIONS_BEING_SERVED = "com.splout.db.versionsBeingServed";
  public static final String KEY_FOR_VERSIONS_BEING_SERVED = "versions";
  // Map<String, String> that keeps the registry of which DNodes are available currently
  public static final String DNODES = "com.splout.db.dnodes";
  // Distributed CountDownLatch prefix name used as barrier to wait for DNodes. Used in
  // conjuntion with the version as postfix
  public static final String GLOBAL_DEPLOY_COUNT_DOWN = "com.splout.db.deploy.countdown-";
  // Error panel used in deployment to inform that the deployment on a DNode failed. This is
  // a prefix, and the version is used as postfix. Key is DNode, value is error explanation.
  public static final String GLOBAL_DEPLOY_ERROR_PANEL = "com.splout.db.deploy.errorPanel-";
  // A Panel to put basic state information about deployments: ongoing / finished / failed. Key is version, value is
  // state.
  public static final String DEPLOYMENTS_STATUS_PANEL = "com.splout.db.deployments.statusPanel";
  // A log panel where we add log messages related to a deployment. This is a prefix, and the version is used
  // as a postfix.
  public static final String GLOBAL_DEPLOY_LOG_PANEL = "com.splout.db.deployments.logPanel-";
  // A panel where a deploy configuration and some basic info like starting time is persisted.
  public static final String GLOBAL_DEPLOY_INFO_PANEL = "com.splout.db.deployments.infoPanel";
  // A panel where each DNode can log the speed / progress of the Deploy on its side. This is a prefix, and the version
  // is used
  // as a postfix.
  public static final String GLOBAL_DEPLOY_SPEED_PANEL = "com.splout.db.deployments.speedPanel-";
  // Version generator. Generates unique version id across the cluster
  public static final String VERSION_GENERATOR = "com.splout.db.versionGenerator";
  // Key for #getDNodeReplicaBalanceActionsSet()
  public static final String DNODE_REPLICA_BALANCE_ACTIONS_SET = "com.splout.db.replicaBalanceActions";

  private HazelcastInstance hz;

  // A JVM-local static flag for unit testing (not associated with Hazelcast) set / unset by {@link
  // com.splout.db.qnode.Deployer}
  // This is only to be used for testing purposes. It is an integer since there might be more than one deploy happening
  // at once.
  public static final AtomicInteger DEPLOY_IN_PROGRESS = new AtomicInteger(0);

  private final static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");

  public CoordinationStructures(HazelcastInstance hz) {
    this.hz = hz;
  }

  /**
   * Returns the set that shows what {@link ReplicaBalancer.BalanceAction}s are currently being performed in the
   * cluster. If some QNode wants to trigger a balance action it has to update this map only in the case that the key
   * doesn't already exist.
   * <p/>
   */
  public IMap<ReplicaBalancer.BalanceAction, String> getDNodeReplicaBalanceActionsSet() {
    return hz.getMap(CoordinationStructures.DNODE_REPLICA_BALANCE_ACTIONS_SET);
  }

  /**
   * Map<String, Map<String, Long>> that keeps the version that is being served currently for each tablespace It only
   * keeps one entry in the map with the full versions, keyed by {@link #KEY_FOR_VERSIONS_BEING_SERVED}. This is done
   * that way, instead having one entry per tablespace because of atomicity: Each time one, or severals tablespaces
   * versions have changes, we receive ALL CHANGES all together.
   */
  public IMap<String, Map<String, Long>> getVersionsBeingServed() {
    return hz.getMap(CoordinationStructures.VERSIONS_BEING_SERVED);
  }

  /**
   * Returns a map with the current version being served per each tablespace. BE CAREFUL: this map is not "managed" by
   * Hazelcast, so changes to it are not shared to the rest of the cluster. If you want so, use
   * {@link #updateVersionsBeingServed(Map, Map)}. It can be null at the startup.
   */
  @SuppressWarnings("unchecked")
  public Map<String, Long> getCopyVersionsBeingServed() {
    return (Map<String, Long>) hz.getMap(CoordinationStructures.VERSIONS_BEING_SERVED).get(
        KEY_FOR_VERSIONS_BEING_SERVED);
  }

  /**
   * Updates the versions being served in the cluster. You should provide the old version you based on to create the new
   * version. If the old version that you used differs with the current version on the cluster, then the update will
   * fail. In this case, you should load the new "oldVersion", reconstruct your new version based on it, and retry. <br>
   * If oldVersion is null, then we imagine that the value was not present in the map. BE CAREFUL: If null is present in
   * the map, the update will fail.
   */
  public boolean updateVersionsBeingServed(Map<String, Long> oldVersion, Map<String, Long> newVersion) {
    IMap<String, Map<String, Long>> ver = getVersionsBeingServed();
    boolean success;
    if (oldVersion == null) {
      success = (ver.putIfAbsent(KEY_FOR_VERSIONS_BEING_SERVED, newVersion) != null);
    } else {
      success = ver.replace(KEY_FOR_VERSIONS_BEING_SERVED, oldVersion, newVersion);
    }
    return success;
  }

  /**
   * Map<String, String> that keeps the registry of which DNodes are available currently. Related with
   * {@link DistributedRegistry}
   */
  public IMap<String, DNodeInfo> getDNodes() {
    return hz.getMap(CoordinationStructures.DNODES);
  }

  /**
   * A reasonable method for generating unique version ids: combining a timestamp (in seconds) with a distributed unique
   * Id from Hazelcast. In this way if the Hazelcast counter is restarted (for example after a cluster shutdown or
   * restart) then the Ids will still be unique afterwards.
   */
  public long uniqueVersionId() {
    IdGenerator idGenerator = hz.getIdGenerator(CoordinationStructures.VERSION_GENERATOR);
    long id = idGenerator.newId();
    int timeSeconds = (int) (System.currentTimeMillis() / 1000);
    long paddedId = id << 32;
    return paddedId + timeSeconds;
  }

  /**
   * Returns a {@link ICountDownLatch} used to know when a deploy process has finished. The version is used to compose
   * the name of the latch, so make it unique for each deployment.
   */
  public ICountDownLatch getCountDownLatchForDeploy(long version) {
    return hz.getCountDownLatch(GLOBAL_DEPLOY_COUNT_DOWN + version);
  }

  /**
   * Returns a map used as panel to publish errors of deployments on DNodes. The key is the DNode id. QNode is informed
   * on errors in DNodes using this map.
   */
  public IMap<String, String> getDeployErrorPanel(long version) {
    return hz.getMap(GLOBAL_DEPLOY_ERROR_PANEL + version);
  }

  /**
   * Returns a map used as panel to publish the speed / progress of each DNode. The key is the DNode id. The value is a
   * string message.
   */
  public IMap<String, String> getDeploySpeedPanel(long version) {
    return hz.getMap(GLOBAL_DEPLOY_SPEED_PANEL + version);
  }

  /**
   * Shortcut method that can be used by DNodes to log the progress of a deploy. It uses a map so it makes sure only one
   * message per DNode is kept. This is convenient since each DNode may report progress every few minutes so we don't
   * end up with a huge list of messages. The rest of the deploy history is logged in another map (see
   * {@link #logDeployMessage(long, String)})
   */
  public void logDeploySpeed(long version, String dnode, String message) {
    hz.getMap(GLOBAL_DEPLOY_SPEED_PANEL + version).put(dnode,
        dateFormat.format(new Date()) + " - " + message);
  }

  /**
   * A Panel to put state information about deployments: ongoing / finished / failed, etc.
   */
  public IMap<Long, DeployStatus> getDeploymentsStatusPanel() {
    return hz.getMap(DEPLOYMENTS_STATUS_PANEL);
  }

  /**
   * A Panel where the basic info and configuration of a deployment can be persisted.
   */
  public IMap<Long, DeployInfo> getDeployInfoPanel() {
    return hz.getMap(GLOBAL_DEPLOY_INFO_PANEL);
  }

  /**
   * Returns a Set used as panel to publish log information of deployments. If {@link #getDeployErrorPanel(long)} can be
   * thought of as a "standardError", this would be a general "standardOut" logging facility for deployment processes.
   * The one publishing info here will normally be the {@link Deployer}.
   */
  public ISet<String> getDeployLogPanel(long version) {
    return hz.getSet(GLOBAL_DEPLOY_LOG_PANEL + version);
  }

  /**
   * Shortcut for using the {@link #getDeployErrorPanel(long)} map. It puts a log message and it appends the current
   * date.
   */
  public void logDeployMessage(long version, String logMessage) {
    hz.getSet(GLOBAL_DEPLOY_LOG_PANEL + version).add(dateFormat.format(new Date()) + " - " + logMessage);
  }
}