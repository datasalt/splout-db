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
import com.splout.db.common.SploutConfiguration;
import com.splout.db.hazelcast.HazelcastConfigBuilder.HazelcastConfigBuilderException;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class TestHazelcastPersistence {

  public final static String PERSISTENCE_FOLDER = "test-" + TestHazelcastPersistence.class.getName();

  @AfterClass
  @BeforeClass
  public static void cleanUp() throws IOException {
    FileUtils.deleteDirectory(new File(PERSISTENCE_FOLDER));
  }

  @Test
  @Ignore // Persistence now being handled outside Hazelcast, this test may be removed
  public void test() throws HazelcastConfigBuilderException, InterruptedException {
    try {
      assertFalse(new File(PERSISTENCE_FOLDER).exists());

      SploutConfiguration config = SploutConfiguration.getTestConfig();
      config.setProperty(HazelcastProperties.HZ_PERSISTENCE_FOLDER, PERSISTENCE_FOLDER);
      HazelcastInstance hz = Hazelcast.newHazelcastInstance(HazelcastConfigBuilder.build(config));

      CoordinationStructures str = new CoordinationStructures(hz);
      str.getVersionsBeingServed().put("foo1", new HashMap<String, Long>());
      str.getVersionsBeingServed().put("foo2", new HashMap<String, Long>());

      Map<String, Long> map1 = str.getVersionsBeingServed().get("foo1");
      map1.put("foo1", 10l);
      Map<String, Long> map2 = str.getVersionsBeingServed().get("foo2");
      map2.put("foo2", 20l);

      str.getVersionsBeingServed().put("foo1", map1);
      str.getVersionsBeingServed().put("foo2", map2);

      assertTrue(new File(PERSISTENCE_FOLDER).exists());

      hz = Hazelcast.newHazelcastInstance(HazelcastConfigBuilder.build(config));
      str = new CoordinationStructures(hz);

      assertEquals(10l, (long) str.getVersionsBeingServed().get("foo1").get("foo1"));
      assertEquals(20l, (long) str.getVersionsBeingServed().get("foo2").get("foo2"));

    } finally {
      Hazelcast.shutdownAll();
    }

  }
}
