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

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.splout.db.common.SploutConfiguration;
import com.splout.db.hazelcast.HazelcastConfigBuilder.HazelcastConfigBuilderException;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.UnknownHostException;

import static org.junit.Assert.*;

public class TestHazelcastUtils {

  @BeforeClass
  public static void init() throws Exception {
    Hazelcast.shutdownAll();
  }

  @After
  public void cleanup() throws Exception {
    Hazelcast.shutdownAll();
  }

  @Test
  public void testGetHZAddress() throws UnknownHostException {
    Member m = new MemberImpl(new Address("127.0.0.1", 123), true);
    assertEquals("/127.0.0.1:123", HazelcastUtils.getHZAddress(m));
  }

  @Test
  public void testIsOneOfOldestMembers() throws UnknownHostException, HazelcastConfigBuilderException {
    HazelcastInstance h1 = Hazelcast.newHazelcastInstance(HazelcastConfigBuilder.build(SploutConfiguration.getTestConfig()));
    HazelcastInstance h2 = Hazelcast.newHazelcastInstance(HazelcastConfigBuilder.build(SploutConfiguration.getTestConfig()));

    assertTrue(HazelcastUtils.isOneOfOldestMembers(h1, h1.getCluster().getLocalMember(), 1));
    assertFalse(HazelcastUtils.isOneOfOldestMembers(h1, h2.getCluster().getLocalMember(), 1));
    assertTrue(HazelcastUtils.isOneOfOldestMembers(h1, h1.getCluster().getLocalMember(), 2));
    assertTrue(HazelcastUtils.isOneOfOldestMembers(h1, h2.getCluster().getLocalMember(), 2));
    assertFalse(HazelcastUtils.isOneOfOldestMembers(h1, h1.getCluster().getLocalMember(), 0));
  }
}
