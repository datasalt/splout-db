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
import com.splout.db.common.TestUtils;
import com.splout.db.hazelcast.CoordinationStructures;
import com.splout.db.hazelcast.HazelcastConfigBuilder;
import com.splout.db.qnode.beans.SwitchVersionRequest;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestRollback {

  @After
  public void cleanUp() throws IOException {
    TestUtils.cleanUpTmpFolders(this.getClass().getName(), 2);
  }

  @Test
  public void testSimpleChange() throws Throwable {
    final QNodeHandler handler = new QNodeHandler();
    SploutConfiguration config = SploutConfiguration.getTestConfig();
    try {

      HazelcastInstance hz = Hazelcast.newHazelcastInstance(HazelcastConfigBuilder.build(config));
      CoordinationStructures coord = new CoordinationStructures(hz);
      /*
       * Create 5 successful versions. current-version will be set to last one.
			 */

      // TODO

//			coord.getTablespaces().put(new TablespaceVersion("t1", 0l), new Tablespace(null, null, 0l, 0l));
//			coord.getTablespaces().put(new TablespaceVersion("t1", 1l), new Tablespace(null, null, 1l, 0l));
//			coord.getTablespaces().put(new TablespaceVersion("t1", 2l), new Tablespace(null, null, 2l, 0l));
//			coord.getTablespaces().put(new TablespaceVersion("t1", 3l), new Tablespace(null, null, 3l, 0l));
//			coord.getTablespaces().put(new TablespaceVersion("t1", 4l), new Tablespace(null, null, 4l, 0l));

      Map<String, Long> versionsBeingServed = new HashMap<String, Long>();
      versionsBeingServed.put("t1", 4l);
      coord.getVersionsBeingServed().put(CoordinationStructures.VERSIONS_BEING_SERVED, versionsBeingServed);

      handler.init(config);

      List<SwitchVersionRequest> rRequest = new ArrayList<SwitchVersionRequest>();
      SwitchVersionRequest theRequest = new SwitchVersionRequest("t1", 3);
      rRequest.add(theRequest);
      handler.rollback(rRequest);

      new TestUtils.NotWaitingForeverCondition() {
        @Override
        public boolean endCondition() {
          return handler.getContext().getCurrentVersionsMap().get("t1") != null &&
              handler.getContext().getCurrentVersionsMap().get("t1") == 3l;
        }
      }.waitAtMost(5000);

      assertEquals(3l, (long) coord.getCopyVersionsBeingServed().get("t1"));
			
			/*
			 * Changing our mind : now back to previous version 4
			 */
      theRequest = new SwitchVersionRequest("t1", 4);
      rRequest = new ArrayList<SwitchVersionRequest>();
      rRequest.add(theRequest);
      handler.rollback(rRequest);

      new TestUtils.NotWaitingForeverCondition() {
        @Override
        public boolean endCondition() {
          return handler.getContext().getCurrentVersionsMap().get("t1") != null &&
              handler.getContext().getCurrentVersionsMap().get("t1") == 4l;
        }
      }.waitAtMost(5000);

      assertEquals(4l, (long) coord.getCopyVersionsBeingServed().get("t1"));
    } finally {
      handler.close();
      Hazelcast.shutdownAll();
    }
  }

  @Test
  public void testMultiRollback() throws Throwable {
    // Here we will have more than one tablespace and we will rollback them all
    final QNodeHandler handler = new QNodeHandler();
    SploutConfiguration config = SploutConfiguration.getTestConfig();
    try {
      HazelcastInstance hz = Hazelcast.newHazelcastInstance(HazelcastConfigBuilder.build(config));
      CoordinationStructures coord = new CoordinationStructures(hz);
			
			/*
			 * Create 5 successful versions. current-version will be set to last one.
			 */

      // TODO

//			for(int i = 0; i < 5; i++) {
//				coord.getTablespaces().put(new TablespaceVersion("t1", i), new Tablespace(null, null, i, 0l));
//				coord.getTablespaces().put(new TablespaceVersion("t2", i), new Tablespace(null, null, i, 0l));
//				coord.getTablespaces().put(new TablespaceVersion("t3", i), new Tablespace(null, null, i, 0l));
//			}

      // T1 -> current version 4
      // T2 -> current version 3
      // T3 -> current version 2
      Map<String, Long> versionsBeingServed = new HashMap<String, Long>();
      versionsBeingServed.put("t1", 4l);
      versionsBeingServed.put("t2", 3l);
      versionsBeingServed.put("t3", 2l);
      coord.getVersionsBeingServed().put(CoordinationStructures.VERSIONS_BEING_SERVED, versionsBeingServed);

      handler.init(config);

      List<SwitchVersionRequest> rRequest = new ArrayList<SwitchVersionRequest>();

      // T1 -> rollback to version 2
      // T2 -> rollback to version 1
      // T3 -> rollback to version 0
      SwitchVersionRequest theRequest = new SwitchVersionRequest("t1", 2l);
      rRequest.add(theRequest);
      theRequest = new SwitchVersionRequest("t2", 1l);
      rRequest.add(theRequest);
      theRequest = new SwitchVersionRequest("t3", 0l);
      rRequest.add(theRequest);

      handler.rollback(rRequest);

      new TestUtils.NotWaitingForeverCondition() {
        @Override
        public boolean endCondition() {
          return
              handler.getContext().getCurrentVersionsMap().get("t1") != null &&
                  handler.getContext().getCurrentVersionsMap().get("t1") == 2l;
        }
      }.waitAtMost(5000);

      assertEquals(2l, (long) coord.getCopyVersionsBeingServed().get("t1"));
      assertEquals(1l, (long) coord.getCopyVersionsBeingServed().get("t2"));
      assertEquals(0l, (long) coord.getCopyVersionsBeingServed().get("t3"));

      assertEquals(2l, (long) handler.getContext().getCurrentVersionsMap().get("t1"));
      assertEquals(1l, (long) handler.getContext().getCurrentVersionsMap().get("t2"));
      assertEquals(0l, (long) handler.getContext().getCurrentVersionsMap().get("t3"));

    } finally {
      handler.close();
      Hazelcast.shutdownAll();
    }
  }
}
