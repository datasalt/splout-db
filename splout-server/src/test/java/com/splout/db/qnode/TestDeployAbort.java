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
import com.splout.db.common.PartitionMap;
import com.splout.db.common.ReplicationEntry;
import com.splout.db.common.SploutConfiguration;
import com.splout.db.common.TestUtils;
import com.splout.db.dnode.DNode;
import com.splout.db.dnode.DNodeHandler;
import com.splout.db.qnode.beans.DeployRequest;
import com.splout.db.thrift.DeployAction;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * This test asserts that the deploy mechanism can be canceled (aborted) when one DNode fails. We will have 2 DNodes:
 * one will fail and the other one will wait forever. In this way we will test that the other DNode is cancelled too.
 */
public class TestDeployAbort {

  // This folder should never be created
  // So that the DNode will fail when trying to deploy it
  public final static String DEPLOY_FOLDER_THAT_DOESNT_EXIST = "deploy-folder-that-doesnt-exist";

  @After
  public void cleanUp() throws IOException {
    TestUtils.cleanUpTmpFolders(this.getClass().getName(), 2);
  }

  /*
   * This DNodeHandler is a normal DNodeHandler except that when it receives a deploy() order it will just stuck in a
   * Thread.sleep(FOREVER). This way we can test deploy cancellation to DNodes: we set a flag if the Thread.sleep() has
   * been interrupted.
   */
  public static class StuckInDeployHandler extends DNodeHandler {

    public static long FOREVER = Long.MAX_VALUE;
    public boolean interrupted = false;

    @Override
    protected DeployRunnable newDeployRunnable(List<DeployAction> deployActions, long version) {
      return new DeployRunnable(deployActions, version) {
        @Override
        public void run() {
          try {
            Thread.sleep(FOREVER);
          } catch (InterruptedException e) {
            interrupted = true;
          }
        }
      };
    }
  }

  @Test
  public void testDeployFinalizing() throws Throwable {
    QNodeHandler handler = new QNodeHandler();
    SploutConfiguration config = SploutConfiguration.getTestConfig();
    SploutConfiguration config1 = SploutConfiguration.getTestConfig();
    SploutConfiguration config2 = SploutConfiguration.getTestConfig();
    // since #12 and #7 we must disable replica balancing which is enabled for testing to fail the deploy
    config.setProperty(QNodeProperties.REPLICA_BALANCE_ENABLE, false);
    config.setProperty(QNodeProperties.DEPLOY_SECONDS_TO_CHECK_ERROR, 1); // this has to be quick for testing
    DNodeHandler dHandler = new DNodeHandler();
    DNode dnode1 = TestUtils
        .getTestDNode(config1, dHandler, "dnode-" + this.getClass().getName() + "-1");

    StuckInDeployHandler stuck = new StuckInDeployHandler();
    DNode dnode2 = TestUtils.getTestDNode(config2, stuck, "dnode-" + this.getClass().getName() + "-2");

    try {
      handler.init(config);

      ReplicationEntry repEntry1 = new ReplicationEntry(0, dnode1.getAddress(), dnode2.getAddress());

      DeployRequest deployRequest1 = new DeployRequest();
      deployRequest1.setTablespace("partition1");
      deployRequest1.setPartitionMap(PartitionMap.oneShardOpenedMap().getPartitionEntries());
      deployRequest1.setReplicationMap(Arrays.asList(repEntry1));

      File fakeDeployFolder = new File(DEPLOY_FOLDER_THAT_DOESNT_EXIST);
      // Remember we don't create the folder on purpose for making the DNode fail
      deployRequest1.setData_uri(fakeDeployFolder.toURI().toString());

      List<DeployRequest> l = new ArrayList<DeployRequest>();
      l.add(deployRequest1);

      handler.deploy(l);

      Thread.sleep(3000);

      assertEquals(0, handler.getContext().getTablespaceVersionsMap().size());

      // the other DNode should have been interrupted
      assertEquals(true, stuck.interrupted);

      // everything OK
    } finally {
      handler.close();
      dnode1.stop();
      dnode2.stop();
      Hazelcast.shutdownAll();
    }
  }
}
