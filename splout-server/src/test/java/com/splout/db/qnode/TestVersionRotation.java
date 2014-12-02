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

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.splout.db.common.SploutConfiguration;
import com.splout.db.common.Tablespace;
import com.splout.db.common.TestUtils;
import com.splout.db.hazelcast.CoordinationStructures;
import com.splout.db.hazelcast.HazelcastConfigBuilder;
import com.splout.db.hazelcast.TablespaceVersion;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestVersionRotation {

  @After
  public void cleanUp() throws IOException {
    TestUtils.cleanUpTmpFolders(this.getClass().getName(), 2);
  }

  @Test
  public void testVersionRotation() throws Throwable {
    // Test that QNodeHandler reads the version list for each tablespace.
    // We will create more versions that needed to assert that QNode deletes the unneeded ones.
    QNodeHandler handler = new QNodeHandler();
    SploutConfiguration config = SploutConfiguration.getTestConfig();
    handler.init(config);
    config.setProperty(QNodeProperties.VERSIONS_PER_TABLESPACE, 5);
    try {

      HazelcastInstance hz = Hazelcast.newHazelcastInstance(HazelcastConfigBuilder.build(config));
      CoordinationStructures coord = new CoordinationStructures(hz);

      handler.init(config);

      for (int i = 0; i < 8; i++) {
        handler.getContext().getTablespaceVersionsMap().put(new TablespaceVersion("t1", i), new Tablespace(null, null, i, 0l));
      }

      Map<String, Long> versionsBeingServed = new HashMap<String, Long>();
      versionsBeingServed.put("t1", 6l);
      coord.getVersionsBeingServed().put(CoordinationStructures.KEY_FOR_VERSIONS_BEING_SERVED, versionsBeingServed);

      Thread.sleep(300);

      List<com.splout.db.thrift.TablespaceVersion> tablespacesToRemove = handler.getContext().synchronizeTablespaceVersions();

			/*
       * Because current version = 6 and QNodeProperties.VERSIONS_PER_TABLESPACE = 5 there will be exactly 5 versions up
			 * to the 6 (including it) : 2, 3, 4, 5, 6. So, version 0 and 1 should have been deleted. Version 7 is kept
			 * because it's after current version (we don't want to mess with it - maybe it's a deployment in progress).
			 */
      assertEquals(2, tablespacesToRemove.size());
      Set<Long> versionsDeleted = new HashSet<Long>();
      for (com.splout.db.thrift.TablespaceVersion tVersion : tablespacesToRemove) {
        versionsDeleted.add(tVersion.getVersion());
      }
      assertTrue(versionsDeleted.contains(0l));
      assertTrue(versionsDeleted.contains(1l));

    } finally {
      handler.close();
      Hazelcast.shutdownAll();
    }
  }
}
