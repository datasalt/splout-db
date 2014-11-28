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

import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.*;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.instance.GroupProperties;
import com.splout.db.common.SploutConfiguration;
import com.splout.db.common.TestUtils;
import com.splout.db.hazelcast.HazelcastConfigBuilder.HazelcastConfigBuilderException;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.splout.db.hazelcast.HazelcastUtils.getHZAddress;
import static org.junit.Assert.*;

public class TestDistributedRegistry {

  @BeforeClass
  public static void init() throws Exception {
    Hazelcast.shutdownAll();
  }

  @After
  public void cleanup() throws Exception {
    Hazelcast.shutdownAll();
  }

  protected static Config buildHZConfig(boolean multicast) throws HazelcastConfigBuilderException {
    SploutConfiguration config = SploutConfiguration.getTestConfig();
    Config c = HazelcastConfigBuilder.build(config);
    c.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(multicast);
    c.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(!multicast);
    if (!multicast) {
      c.getNetworkConfig().setPortAutoIncrement(false);
    }
    c.setProperty(GroupProperties.PROP_MERGE_FIRST_RUN_DELAY_SECONDS, "0"); // 10
    c.setProperty(GroupProperties.PROP_MERGE_NEXT_RUN_DELAY_SECONDS, "0"); // 5
    return c;
  }

  /**
   * When split-brain, after merge, joining nodes must re-register themselves. Checking.
   */
  @SuppressWarnings("deprecation")
  @Test(timeout = 180000)
  public void testReregisterAfterMerge() throws Exception {

    // This port selection ensures that when h3 restarts it will try to join h4
    // instead of joining the nodes in cluster one
    Config c1 = buildHZConfig(false);
    c1.getNetworkConfig().setPort(15701);
    Config c2 = buildHZConfig(false);
    c2.getNetworkConfig().setPort(15702);
    Config c3 = buildHZConfig(false);
    c3.getNetworkConfig().setPort(15703);
    Config c4 = buildHZConfig(false);
    c4.getNetworkConfig().setPort(15704);

    List<String> clusterOneMembers = Arrays.asList("127.0.0.1:15701", "127.0.0.1:15702",
        "127.0.0.1:15703");
    List<String> clusterTwoMembers = Arrays.asList("127.0.0.1:15704");

    c1.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(clusterOneMembers);
    c2.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(clusterOneMembers);
    c3.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(clusterOneMembers);
    c4.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(clusterTwoMembers);

    c4.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");

    final CountDownLatch latch = new CountDownLatch(1);
    c4.addListenerConfig(new ListenerConfig(new LifecycleListener() {
      public void stateChanged(final LifecycleEvent event) {
        if (event.getState() == LifecycleState.MERGED) {
          System.out.println("h4 restarted");
          latch.countDown();
        }
      }
    }));

    HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c1);
    HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c2);
    HazelcastInstance h3 = Hazelcast.newHazelcastInstance(c3);
    HazelcastInstance h4 = Hazelcast.newHazelcastInstance(c4);

    // We should have two clusters of size three and one
    assertEquals(3, h1.getCluster().getMembers().size());
    assertEquals(3, h2.getCluster().getMembers().size());
    assertEquals(3, h3.getCluster().getMembers().size());
    assertEquals(1, h4.getCluster().getMembers().size());

    DistributedRegistry reg = new DistributedRegistry("reg", "h4", h4, 10, 3);
    reg.disableChecking(true);
    reg.register();

    String h4Member = getHZAddress(h4.getCluster().getLocalMember());
    assertEquals("h4", h4.getMap("reg").get(h4Member));

    // We remove the registration manually... to see if later
    // it is re-registered after the merge.

    h4.getMap("reg").remove(h4Member);
    assertTrue(h4.getMap("reg").get(h4Member) == null);
    assertTrue(h3.getMap("reg").get(h4Member) == null);

    List<String> allMembers = Arrays.asList("127.0.0.1:15701", "127.0.0.1:15704", "127.0.0.1:15703",
        "127.0.0.1:15702");

		/*
     * This simulates restoring a network connection between h4 and the other cluster.
		 */
    h4.getConfig().getNetworkConfig().getJoin().getTcpIpConfig().setMembers(allMembers);

    latch.await(60, TimeUnit.SECONDS);

    // Both nodes from cluster two should have joined cluster one
    assertEquals(4, h1.getCluster().getMembers().size());
    assertEquals(4, h2.getCluster().getMembers().size());
    assertEquals(4, h3.getCluster().getMembers().size());
    assertEquals(4, h4.getCluster().getMembers().size());

    Thread.sleep(1000 * 3);

    // Now, h4 must be registered
    assertEquals("h4", h4.getMap("reg").get(h4Member));
    assertEquals("h4", h3.getMap("reg").get(h4Member));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testCrossRequest() throws Exception {

    Config c1 = buildHZConfig(false);
    c1.getNetworkConfig().setPort(15701);
    Config c2 = buildHZConfig(false);
    c2.getNetworkConfig().setPort(15702);

    List<String> clusterMembers = Arrays.asList("127.0.0.1:15701", "127.0.0.1:15702");
    c1.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(clusterMembers);
    c2.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(clusterMembers);

    HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c1);
    HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c2);

    assertEquals(2, h1.getCluster().getMembers().size());
    assertEquals(2, h2.getCluster().getMembers().size());

    DistributedRegistry reg1 = new DistributedRegistry("reg", "h1", h1, 10, 3);
    reg1.disableChecking(true);
    reg1.register();

    DistributedRegistry reg2 = new DistributedRegistry("reg", "h2", h2, 10, 3);
    reg2.disableChecking(true);
    reg2.register();

    assertEquals(2, h2.getMap("reg").size());

    String memberOne = null;
    String memberTwo = null;
    for (Member m : h2.getCluster().getMembers()) {
      if (m.getInetSocketAddress().getPort() == 15701) {
        memberOne = getHZAddress(m);
      } else {
        memberTwo = getHZAddress(m);
        ;
      }
    }
    System.out.println(memberOne);
    assertEquals("h1", h2.getMap("reg").get(memberOne));
    assertEquals("h1", h1.getMap("reg").get(memberOne));
    assertEquals("h2", h2.getMap("reg").get(memberTwo));
    assertEquals("h2", h1.getMap("reg").get(memberTwo));

    memberOne = null;
    memberTwo = null;
    for (Member m : h1.getCluster().getMembers()) {
      if (m.getInetSocketAddress().getPort() == 15701) {
        memberOne = getHZAddress(m);
        ;
      } else {
        memberTwo = getHZAddress(m);
        ;
      }
    }
    System.out.println(memberOne);
    assertEquals("h1", h2.getMap("reg").get(memberOne));
    assertEquals("h1", h1.getMap("reg").get(memberOne));
    assertEquals("h2", h2.getMap("reg").get(memberTwo));
    assertEquals("h2", h1.getMap("reg").get(memberTwo));

  }

  /**
   * When split-brain some members could have lost their registry information. This test test that the checker works.
   */
  @Test
  public void testChecking() throws Exception {
    testChecking(true);
  }

  /**
   * When split-brain some members could have lost their registry information. This test test that if checker is
   * disabled, it does not work.
   */
  @Test
  public void testNoChecking() throws Throwable {
    testChecking(false);
  }

  public void testChecking(final boolean enabledChecking) throws Exception {

    Config c1 = buildHZConfig(true);
    Config c2 = buildHZConfig(true);

    final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c1);

    DistributedRegistry reg = new DistributedRegistry("reg", "stuff", h1, 0, 1);
    reg.disableChecking(!enabledChecking);
    reg.register();
    // We call changeInfo, to check that the changeInfo is algo changing the in memory stored information,
    reg.changeInfo("h1");

    // We remove the registration manually... to see if later
    // it is re-registered after the join of a new member.
    final String member = getHZAddress(h1.getCluster().getLocalMember());
    h1.getMap("reg").remove(member);

    // Now new member arrives
    HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c2);

    // We should have two clusters of size three and one
    assertEquals(2, h1.getCluster().getMembers().size());

    Thread.sleep(50);
    if (enabledChecking) {
      assertEquals("h1", h1.getMap("reg").get(member));
    } else {
      assertNull(h1.getMap("reg").get(member));
    }

    // Now the member leaves
    h1.getMap("reg").remove(member);
    h2.getLifecycleService().shutdown();

    new TestUtils.NotWaitingForeverCondition() {

      @Override
      public boolean endCondition() {
        if (enabledChecking) {
          return "h1".equals(h1.getMap("reg").get(member));
        } else {
          return h1.getMap("reg").get(member) == null;
        }
      }
    }.waitAtMost(7000);
  }

}
