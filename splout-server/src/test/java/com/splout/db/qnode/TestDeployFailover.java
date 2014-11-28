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

import com.hazelcast.core.Hazelcast;
import com.splout.db.common.*;
import com.splout.db.dnode.DNode;
import com.splout.db.dnode.DNodeHandler;
import com.splout.db.hazelcast.DNodeInfo;
import com.splout.db.qnode.beans.DeployRequest;
import com.splout.db.thrift.DNodeException;
import com.splout.db.thrift.DeployAction;
import com.splout.db.thrift.PartitionMetadata;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Since #12 and #7 deploy can have failover if replica balancing is enabled.
 * In that case replicas will be fully replicated after deploy.
 */
public class TestDeployFailover {

  // This folder should never be created
  // So that the DNode will fail when trying to deploy it
  public final static String DEPLOY_FOLDER_THAT_DOESNT_EXIST = "deploy-folder-that-doesnt-exist";

  @After
  public void cleanUp() throws IOException {
    TestUtils.cleanUpTmpFolders(this.getClass().getName(), 2);
  }

  public static class SucceedingDNodeHandler extends DNodeHandler {

    @Override
    public String deploy(List<DeployAction> deployActions, final long version) throws DNodeException {
      synchronized (deployLock) {
        deployInProgress.incrementAndGet();
        deployThread.submit(new Runnable() {
          @Override
          public void run() {
            // I always succeed.
            try {
              // Create the necessary disk structures so this node's partition is promoted to Hazelcast
              getLocalStorageFolder("t1", 0, version).mkdirs();
              ThriftWriter writer = new ThriftWriter(getLocalMetadataFile("t1", 0, version));
              PartitionMetadata metadata = new PartitionMetadata();
              // we need to set expected replication factor, otherwise replica balancing will not trigger for this test
              metadata.setNReplicas(2);
              writer.write(metadata);
              writer.close();
              new File(getLocalStorageFolder("t1", 0, version), "0.db").createNewFile();
              // promote info to Hazelcast
              getDnodesRegistry().changeInfo(new DNodeInfo(config));
            } catch (IOException e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            }
            // countdown for finishing deployment
            getCoord().getCountDownLatchForDeploy(version).countDown();
          }
        });
      }
      return "FOO";
    }
  }

  @Test
  public void testDeployFailover() throws Throwable {
    final QNodeHandler handler = new QNodeHandler();
    SploutConfiguration config = SploutConfiguration.getTestConfig();
    SploutConfiguration config1 = SploutConfiguration.getTestConfig();
    SploutConfiguration config2 = SploutConfiguration.getTestConfig();

    // Replica balancing should already be enabled for tests but we just make it explicit here -->
    config.setProperty(QNodeProperties.REPLICA_BALANCE_ENABLE, true);
    config.setProperty(QNodeProperties.WARMING_TIME, 0);
    config.setProperty(QNodeProperties.DEPLOY_SECONDS_TO_CHECK_ERROR, 1); // this has to be quick for testing

    DNodeHandler dHandler = new DNodeHandler();
    DNode dnode1 = TestUtils
        .getTestDNode(config1, dHandler, "dnode-" + this.getClass().getName() + "-1");

    SucceedingDNodeHandler succeed = new SucceedingDNodeHandler();
    DNode dnode2 = TestUtils.getTestDNode(config2, succeed, "dnode-" + this.getClass().getName() + "-2");

    try {
      handler.init(config);

      ReplicationEntry repEntry1 = new ReplicationEntry(0, dnode1.getAddress(), dnode2.getAddress());

      DeployRequest deployRequest1 = new DeployRequest();
      deployRequest1.setTablespace("t1");
      deployRequest1.setPartitionMap(PartitionMap.oneShardOpenedMap().getPartitionEntries());
      deployRequest1.setReplicationMap(Arrays.asList(repEntry1));

      File fakeDeployFolder = new File(DEPLOY_FOLDER_THAT_DOESNT_EXIST);
      // Remember we don't create the folder on purpose for making the DNode fail
      deployRequest1.setData_uri(fakeDeployFolder.toURI().toString());

      List<DeployRequest> l = new ArrayList<DeployRequest>();
      l.add(deployRequest1);

      handler.deploy(l);

      // wait until deploy finished
      new TestUtils.NotWaitingForeverCondition() {

        @Override
        public boolean endCondition() {
          return handler.getContext().getTablespaceVersionsMap().size() == 1;
        }
      }.waitAtMost(5000);

      // wait until replica balancer re-balanced the under-replicated partition
      new TestUtils.NotWaitingForeverCondition() {

        @Override
        public boolean endCondition() {
          ReplicationMap map = handler.getContext().getTablespaceVersionsMap().values().iterator().next().getReplicationMap();
          return map.getReplicationEntries().get(0).getNodes().size() == 2;
        }
      }.waitAtMost(5000);

      // everything OK
    } finally {
      handler.close();
      dnode1.stop();
      dnode2.stop();
      Hazelcast.shutdownAll();
    }
  }
}
