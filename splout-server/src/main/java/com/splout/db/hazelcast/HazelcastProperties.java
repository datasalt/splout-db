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

public class HazelcastProperties {

  /**
   * Enable this property to true in order to skip the programmatic Hazelcast config facade. Hazelcast configuration will be
   * loaded using the standard mechanisms for Hazelcast config loading (hazelcast.xml in classpath or hazelcast.config env property).
   */
  public static final String USE_DEFAULT_OR_XML_CONFIG = "hz.use.default.or.xml.config";
  /**
   * Modifies the standard Hazelcast port.
   */
  public static final String PORT = "hz.port";
  /**
   * Sets the network interfaces to be used by Hazelcast e.g. 10.10.1.* . Comma separated if more than one.
   */
  public static final String INTERFACES = "hz.interfaces";
  /**
   * Modifies the standard backup count. Affects the replication factor of distributed maps.
   */
  public static final String BACKUP_COUNT = "hz.backup.count";
  /**
   * Use this property to configure Hazelcast join in one or other way. Possible values: MULTICAST, TCP, AWS
   */
  public static final String JOIN_METHOD = "hz.join.method";

  public static enum JOIN_METHODS {
    MULTICAST, TCP, AWS
  }

  /**
   * Comma-separated list of hosts values that will be added to a TCP/IP Hazelcast cluster, if hz.join.method = TCP
   */
  public static final String TCP_CLUSTER = "hz.tcp.cluster"; // comma-separated, default null
  /**
   * Required Member for TCP/IP Hazelcast join method. Sets the required member.
   * If a null value is passed, it means that there is no required member.
   * With a required member configured, the cluster will only start up when this required member
   * is up. Setting the required member can be tricky, since if that member doesn't come up, the cluster won't start.
   * Only one required member is allowed.
   */
  public static final String TCP_CLUSTER_REQUIRED_MEMBER = "hz.tcp.cluster.required.member"; // String, default null
  /**
   * Sets the connection timeout when clients trying to joining by TCP to other
   * servers.
   */
  public static final String TCP_CONNECTION_TIMEOUT_SECONDS = "hz.tcp.connection.timeout.seconds"; // Default 20.
  /**
   * Optional multicast group for MULTICAST join method
   */
  public static final String MULTICAST_GROUP = "hz.multicast.group"; // default null
  /**
   * Optional multicast port for MULTICAST join method
   */
  public static final String MULTICAST_PORT = "hz.multicast.port"; // default null
  /**
   * Optional AWS security group for AWS auto-discovery method
   */
  public static final String AWS_SECURITY_GROUP = "hz.aws.security.group"; // default null
  /**
   * Mandatory AWS access key when using AWS auto-discovery method
   */
  public static final String AWS_KEY = "hz.aws.key"; // default null
  /**
   * Mandatory AWS secret key when using AWS auto-discovery method
   */
  public static final String AWS_SECRET = "hz.aws.secret"; // default null
  /**
   * Folder to be used to persist Hazelcast state information
   * Needed to persist current version information.
   * If not present, no information is stored
   */
  public static final String HZ_PERSISTENCE_FOLDER = "hz.persistent.data.folder";
  /**
   * Hazelcast waits 5 seconds before joining a member. That is good in production
   * because improves the posibilities of joining several members at the same time.
   * But very bad for testing... This property allows you to disable it for testing.
   */
  public static final String DISABLE_WAIT_WHEN_JOINING = "hz.disable.wait.when.joining";
  /**
   * Number of the oldest members leading operations in the cluster.
   * Sometimes only these members answer to events, in order to reduce
   * coordination traffic.
   */
  public static final String OLDEST_MEMBERS_LEADING_COUNT = "hz.oldest.members.leading.count";
  /**
   * Max time, in minutes, to check if the member is registered. This check is used
   * to assure eventual consistency in rare cases of network partitions where replication
   * was not enough to ensure that no data is lost.
   */
  public static final String MAX_TIME_TO_CHECK_REGISTRATION = "hz.registry.max.time.to.check.registration";
}
