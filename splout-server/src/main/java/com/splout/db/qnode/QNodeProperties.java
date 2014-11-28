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
   * The number of seconds to wait before checking each time if a DNode has failed or if timeout has ocurred in the
   * middle of a deploy
   */
  public final static String DEPLOY_SECONDS_TO_CHECK_ERROR = "qnode.deploy.seconds.to.check.error";
  /**
   * The timeout in seconds when waiting the DNodes metadata to spread in the cluster
   * once all of them has finished the downloading.
   */
  public final static String DEPLOY_DNODES_SPREAD_METADATA_TIMEOUT = "qnode.deploy.dnodes.spread.metadata.timeout";
  /**
   * A fixed amount of time (seconds) that this QNode will wait before taking certain actions. For example, a QNode may decide to
   * re-balance under-replicated partitions. But if we are at cluster start, some DNodes may still be connecting or are
   * to connect in the near future. For that it is useful to wait this "warming time" before deciding such things.
   */
  public final static String WARMING_TIME = "qnode.warming.time";
  /**
   * The TTL (in seconds) of balance actions: if they take more than this number of seconds they will be evicted.
   * This is necessary because there are a number of race conditions that might block certain actions (i.e. final DNode
   * going down in the middle of the transfer).
   */
  public final static String BALANCE_ACTIONS_TTL = "qnode.balance.actions.ttl";
  /**
   * Whether or not to enable automatic replica balancing.
   * If automatic replica balancing is enabled, the system will become highly available so that
   * if one replica is down, it will copy other replicas to another DNode so that the affected
   * partitions don't remain under-replicated.
   * As a downside, replica balancing might use more disk space than desired.
   */
  public final static String REPLICA_BALANCE_ENABLE = "qnode.enable.replica.balance";

  /**
   * The time to wait in millis for a dnode connection in pool. It is used when pool is
   * empty because all connections are being used.
   */
  public static final String QNODE_DNODE_POOL_TAKE_TIMEOUT = "qnode.dnode.pool.take.timeout";
}
