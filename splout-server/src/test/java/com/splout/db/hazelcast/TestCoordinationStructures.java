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
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestCoordinationStructures {

  @Test
  public void testUniqueIdGenerator() throws HazelcastConfigBuilderException {
    try {
      SploutConfiguration config = SploutConfiguration.getTestConfig();
      HazelcastInstance hz = Hazelcast.newHazelcastInstance(HazelcastConfigBuilder.build(config));
      CoordinationStructures cS = new CoordinationStructures(hz);

      for (int i = 0; i < 1000; i++) {
        long version1 = cS.uniqueVersionId();
        long version2 = cS.uniqueVersionId();
        assertTrue(version2 > version1);
      }

    } finally {
      Hazelcast.shutdownAll();
    }
  }
}
