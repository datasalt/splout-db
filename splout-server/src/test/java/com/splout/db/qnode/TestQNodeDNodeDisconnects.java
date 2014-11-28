package com.splout.db.qnode;

/*
 * #%L
 * Splout SQL Server
 * %%
 * Copyright (C) 2012 - 2013 Datasalt Systems S.L.
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

import com.splout.db.common.SploutConfiguration;
import com.splout.db.common.TestUtils;
import com.splout.db.dnode.DNode;
import com.splout.db.dnode.DNodeHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * This test makes sure that QNode handles DNode connect / disconnect properly by populating and invalidating the Thrift
 * connection pool to it.
 */
public class TestQNodeDNodeDisconnects {

  @After
  @Before
  public void cleanUp() throws IOException {
    TestUtils.cleanUpTmpFolders(this.getClass().getName(), 4);
  }

  @Test
  public void test() throws Throwable {
    final SploutConfiguration config = SploutConfiguration.getTestConfig();
    final QNodeHandler handler = new QNodeHandler();
    QNode qnode = TestUtils.getTestQNode(config, handler);

    DNodeHandler dNodeHandler1 = new DNodeHandler();

    final SploutConfiguration config1 = SploutConfiguration.getTestConfig();

    DNode dnode1 = TestUtils.getTestDNode(config1, dNodeHandler1, "dnode-" + this.getClass().getName()
        + "-1");
    final String dnode1Address = dnode1.getAddress();

    try {

      assertEquals(handler.getDNodeList().size(), 1);
      // wait until connection pool has been generated
      new TestUtils.NotWaitingForeverCondition() {

        @Override
        public boolean endCondition() {
          return handler.getContext().getThriftClientCache().get(dnode1Address) != null
              && handler.getContext().getThriftClientCache().get(dnode1Address).size() == 40;
        }
      }.waitAtMost(20000);

      dnode1.stop();

      final DNode dnodeAux = dnode1;
      new TestUtils.NotWaitingForeverCondition() {
        @Override
        public boolean endCondition() throws Exception {
          return handler.getDNodeList().size() == 0 &&
              handler.getContext().getThriftClientCache().get(dnodeAux.getAddress()) == null;
        }
      }.waitAtMost(20000);

      dnode1 = TestUtils.getTestDNode(config, dNodeHandler1, "dnode-" + this.getClass().getName() + "-2");

      new TestUtils.NotWaitingForeverCondition() {
        @Override
        public boolean endCondition() throws Exception {
          return handler.getDNodeList().size() == 1;
        }
      }.waitAtMost(20000);

      // wait until connection pool has been regenerated
      new TestUtils.NotWaitingForeverCondition() {

        @Override
        public boolean endCondition() {
          return handler.getContext().getThriftClientCache().get(dnode1Address) != null
              && handler.getContext().getThriftClientCache().get(dnode1Address).size() == 40;
        }
      }.waitAtMost(20000);

    } finally {
      dnode1.stop();
      qnode.close();
    }
  }
}
