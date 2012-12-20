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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.splout.db.common.PartitionMap;
import com.splout.db.common.ReplicationEntry;
import com.splout.db.common.ReplicationMap;
import com.splout.db.common.SploutConfiguration;
import com.splout.db.common.Tablespace;
import com.splout.db.common.TestUtils;
import com.splout.db.dnode.DNode;
import com.splout.db.dnode.DNodeHandler;
import com.splout.db.dnode.DNodeMockHandler;
import com.splout.db.dnode.DNodeProperties;
import com.splout.db.dnode.IDNodeHandler;
import com.splout.db.hazelcast.CoordinationStructures;
import com.splout.db.hazelcast.DNodeInfo;
import com.splout.db.hazelcast.HazelcastConfigBuilder;
import com.splout.db.hazelcast.TablespaceVersion;
import com.splout.db.qnode.beans.DeployRequest;
import com.splout.db.qnode.beans.QueryStatus;
import com.splout.db.thrift.DNodeException;
import com.splout.db.thrift.DeployAction;
import com.splout.db.thrift.RollbackAction;

public class TestQNodeHandler {

	public final static String FAKE_DEPLOY_FOLDER = "fake-deploy-db";

	@After
	@Before
	public void cleanUp() throws IOException {
		FileUtils.deleteDirectory(new File(FAKE_DEPLOY_FOLDER));
		TestUtils.cleanUpTmpFolders(this.getClass().getName(), 6);
	}

	DNodeMockHandler dHandler = new DNodeMockHandler() {
		
		@Override
		public String sqlQuery(String tablespace, long version, int partition, String query) throws DNodeException {
			return "[1]";
		}
		@Override
		public String deploy(List<DeployAction> deployActions, long version) throws DNodeException {
			return "FOO";
		}
		@Override
		public String rollback(List<RollbackAction> rollbackActions, String ignoreMe) throws DNodeException {
			return "FOO";
		}
		@Override
		public String status() throws DNodeException {
			return "FOO";
		}
	};

	@Test
	public void testInitVersionListAndVersionChange() throws Throwable {
		QNodeHandler handler = new QNodeHandler();
		SploutConfiguration config = SploutConfiguration.getTestConfig();
		try {

			HazelcastInstance hz = Hazelcast.newHazelcastInstance(HazelcastConfigBuilder.build(config));
			CoordinationStructures coord = new CoordinationStructures(hz);

			Map<String, Long> versionsBeingServed = new HashMap<String, Long>();
			versionsBeingServed.put("t1", 0l);
			coord.getVersionsBeingServed().put(CoordinationStructures.KEY_FOR_VERSIONS_BEING_SERVED, versionsBeingServed);

			versionsBeingServed.put("t2", 1l);
			coord.getVersionsBeingServed().put(CoordinationStructures.KEY_FOR_VERSIONS_BEING_SERVED, versionsBeingServed);

			handler.init(config);

			Assert.assertEquals(0l, (long) handler.getContext().getCurrentVersionsMap().get("t1"));
			Assert.assertEquals(1l, (long) handler.getContext().getCurrentVersionsMap().get("t2"));

			versionsBeingServed.put("t2", 0l);
			versionsBeingServed.put("t1", 1l);
			coord.getVersionsBeingServed().put(CoordinationStructures.KEY_FOR_VERSIONS_BEING_SERVED, versionsBeingServed);

			Thread.sleep(100);

			Assert.assertEquals(1l, (long) handler.getContext().getCurrentVersionsMap().get("t1"));
			Assert.assertEquals(0l, (long) handler.getContext().getCurrentVersionsMap().get("t2"));

			versionsBeingServed.put("t2", 1l);
			versionsBeingServed.put("t1", 0l);
			coord.getVersionsBeingServed().put(CoordinationStructures.KEY_FOR_VERSIONS_BEING_SERVED, versionsBeingServed);

			Thread.sleep(100);

			Assert.assertEquals(0l, (long) handler.getContext().getCurrentVersionsMap().get("t1"));
			Assert.assertEquals(1l, (long) handler.getContext().getCurrentVersionsMap().get("t2"));
		} finally {
			handler.close();
			Hazelcast.shutdownAll();
		}
	}

	@Test
	public void testDeployEnding() throws Throwable {
		// Test what happens when DNodes complete the deploy process
		final QNodeHandler handler = new QNodeHandler();
		SploutConfiguration config = SploutConfiguration.getTestConfig();
		DNodeHandler dHandler = new DNodeHandler();
		DNode dnode = TestUtils.getTestDNode(config, dHandler, "dnode-" + this.getClass().getName() + "-1");

		try {
			handler.init(config);

			DeployRequest deployRequest1 = new DeployRequest();
			deployRequest1.setTablespace("partition1");
			deployRequest1.setPartitionMap(PartitionMap.oneShardOpenedMap().getPartitionEntries());
			deployRequest1.setReplicationMap(ReplicationMap.oneToOneMap(dnode.getAddress()).getReplicationEntries());

			File fakeDeployFolder = new File(FAKE_DEPLOY_FOLDER);
			fakeDeployFolder.mkdir();
			File deployData = new File(fakeDeployFolder, "0.db");
			deployData.createNewFile();
			deployRequest1.setData_uri(fakeDeployFolder.toURI().toString());

			List<DeployRequest> l = new ArrayList<DeployRequest>();
			l.add(deployRequest1);

			handler.deploy(l);

			new TestUtils.NotWaitingForeverCondition() {
				@Override
				public boolean endCondition() {
					boolean cond1 = handler.getContext().getTablespaceVersionsMap().values().size() == 1;
					boolean cond2 = handler.getContext().getCurrentVersionsMap().get("partition1") != null; 
					return cond1 && cond2;
				}
			}.waitAtMost(5000);

			assertEquals((long) handler.getContext().getTablespaceVersionsMap().keySet().iterator().next().getVersion(),
			    (long) handler.getContext().getCurrentVersionsMap().values().iterator().next());
			// everything OK
		} finally {
			handler.close();
			dnode.stop();
			Hazelcast.shutdownAll();
		}
	}

	@Test
	public void testQuery() throws Throwable {
		QNodeHandler handler = new QNodeHandler();
		handler.init(SploutConfiguration.getTestConfig());

		SploutConfiguration config = SploutConfiguration.getTestConfig();
		DNode dnode = TestUtils.getTestDNode(config, dHandler, "dnode-" + this.getClass().getName() + "-2");
		try {
			ReplicationEntry repEntry = new ReplicationEntry(0, dnode.getAddress());
			Tablespace tablespace1 = new Tablespace(PartitionMap.oneShardOpenedMap(), new ReplicationMap(Arrays.asList(repEntry)), 0l, 0l);
			handler.getContext().getTablespaceVersionsMap().put(new TablespaceVersion("tablespace1", 0l), tablespace1);
			handler.getContext().getCurrentVersionsMap().put("tablespace1", 0l);

			// Query key 2 (> 1 < 10)
			QueryStatus qStatus = handler.query("tablespace1", "2", "SELECT 1;", null);
			Assert.assertEquals(new Integer(0), qStatus.getShard());
			Assert.assertEquals("[1]", qStatus.getResult().toString());
		} finally {
			handler.close();
			dnode.stop();
			Hazelcast.shutdownAll();
		}
	}

	@Test
	public void testQueryManualPartition() throws Throwable {
		// same as testQuery but using manual partition instead of "key"
		QNodeHandler handler = new QNodeHandler();
		handler.init(SploutConfiguration.getTestConfig());

		SploutConfiguration config = SploutConfiguration.getTestConfig();
		DNode dnode = TestUtils.getTestDNode(config, dHandler, "dnode-" + this.getClass().getName() + "-2b");
		try {
			ReplicationEntry repEntry = new ReplicationEntry(0, dnode.getAddress());
			Tablespace tablespace1 = new Tablespace(PartitionMap.oneShardOpenedMap(), new ReplicationMap(Arrays.asList(repEntry)), 0l, 0l);
			handler.getContext().getTablespaceVersionsMap().put(new TablespaceVersion("tablespace1", 0l), tablespace1);
			handler.getContext().getCurrentVersionsMap().put("tablespace1", 0l);

			// Query shard 0
			QueryStatus qStatus = handler.query("tablespace1", null, "SELECT 1;", "0");
			Assert.assertEquals(new Integer(0), qStatus.getShard());
			Assert.assertEquals("[1]", qStatus.getResult().toString());
			
			// Query random partition
		  qStatus = handler.query("tablespace1", null, "SELECT 1;", Querier.PARTITION_RANDOM);
			Assert.assertEquals(new Integer(0), qStatus.getShard());
			Assert.assertEquals("[1]", qStatus.getResult().toString());
			
		} finally {
			handler.close();
			dnode.stop();
			Hazelcast.shutdownAll();
		}
	}
	
	@Test
	public void testMultiDeployFiring() throws Throwable {
		// Same as test deploy firing, but with more than one DNode and different deploy actions
		SploutConfiguration config1 = SploutConfiguration.getTestConfig();

		DNode dnode1 = TestUtils.getTestDNode(config1, new IDNodeHandler() {
			@Override
			public void init(SploutConfiguration config) throws Exception {
			}

			@Override
			public String sqlQuery(String tablespace, long version, int partition, String query) throws DNodeException {
				return null;
			}

			@Override
			public String deploy(List<DeployAction> deployActions, long distributedBarrier) throws DNodeException {
				/*
				 * DNode1 asserts
				 */
				Assert.assertEquals(2, deployActions.size());
				Assert.assertEquals("hdfs://foo/bar1/0.db", deployActions.get(0).getDataURI());
				Assert.assertEquals("hdfs://foo/bar2/0.db", deployActions.get(1).getDataURI());
				Assert.assertEquals("partition1", deployActions.get(0).getTablespace());
				Assert.assertEquals("partition2", deployActions.get(1).getTablespace());
				return "FOO";
			}

			@Override
			public String rollback(List<RollbackAction> rollbackActions, String ignoreMe) throws DNodeException {
				return null;
			}

			@Override
			public String status() throws DNodeException {
				return null;
			}

			@Override
			public void stop() throws Exception {
			}

			@Override
			public void giveGreenLigth() {
			}

			@Override
			public String abortDeploy(long version) throws DNodeException {
				return null;
			}

			@Override
			public String deleteOldVersions(List<com.splout.db.thrift.TablespaceVersion> versions) throws DNodeException {
				return null;
			}

			@Override
      public String testCommand(String command) throws DNodeException {
	      // TODO Auto-generated method stub
	      return null;
      }
		}, "dnode-" + this.getClass().getName() + "-3");

		SploutConfiguration config2 = SploutConfiguration.getTestConfig();
		DNode dnode2 = TestUtils.getTestDNode(config2, new IDNodeHandler() {
			@Override
			public void init(SploutConfiguration config) throws Exception {
			}

			@Override
			public String sqlQuery(String tablespace, long version, int partition, String query) throws DNodeException {
				return null;
			}

			@Override
			public String deploy(List<DeployAction> deployActions, long distributedBarrier) throws DNodeException {
				/*
				 * DNode2 asserts
				 */
				Assert.assertEquals(1, deployActions.size());
				Assert.assertEquals("hdfs://foo/bar1/0.db", deployActions.get(0).getDataURI());
				Assert.assertEquals("partition1", deployActions.get(0).getTablespace());
				return "FOO";
			}

			@Override
			public String rollback(List<RollbackAction> rollbackActions, String ignoreMe) throws DNodeException {
				return null;
			}

			@Override
			public String status() throws DNodeException {
				return null;
			}

			@Override
			public void stop() throws Exception {
			}

			@Override
			public void giveGreenLigth() {
			}

			@Override
			public String abortDeploy(long version) throws DNodeException {
				return null;
			}

			@Override
			public String deleteOldVersions(List<com.splout.db.thrift.TablespaceVersion> versions) throws DNodeException {
				return null;
			}

			@Override
      public String testCommand(String command) throws DNodeException {
	      // TODO Auto-generated method stub
	      return null;
      }
		}, "dnode-" + this.getClass().getName() + "-4");

		QNodeHandler handler = new QNodeHandler();
		try {
			handler.init(config1);
			ReplicationEntry repEntry1 = new ReplicationEntry(0, dnode1.getAddress(), dnode2.getAddress());
			ReplicationEntry repEntry2 = new ReplicationEntry(0, dnode1.getAddress());

			DeployRequest deployRequest1 = new DeployRequest();
			deployRequest1.setTablespace("partition1");
			deployRequest1.setPartitionMap(PartitionMap.oneShardOpenedMap().getPartitionEntries());
			deployRequest1.setReplicationMap(Arrays.asList(repEntry1));
			deployRequest1.setData_uri("hdfs://foo/bar1");

			DeployRequest deployRequest2 = new DeployRequest();
			deployRequest2.setTablespace("partition2");
			deployRequest2.setPartitionMap(PartitionMap.oneShardOpenedMap().getPartitionEntries());
			deployRequest2.setReplicationMap(Arrays.asList(repEntry2));
			deployRequest2.setData_uri("hdfs://foo/bar2");

			List<DeployRequest> l = new ArrayList<DeployRequest>();
			l.add(deployRequest1);
			l.add(deployRequest2);

			handler.deploy(l);
		} finally {
			handler.close();
			dnode1.stop();
			dnode2.stop();
			Hazelcast.shutdownAll();
		}
	}

	@Test
	public void testDeployFiring() throws Throwable {
		// Test the business logic that produces the firing of the deployment (not the continuation of it) For that, we will
		// use dummy DNodeHandlers
		QNodeHandler handler = new QNodeHandler();
		SploutConfiguration config = SploutConfiguration.getTestConfig();

		DNode dnode = TestUtils.getTestDNode(config, new IDNodeHandler() {
			@Override
			public void init(SploutConfiguration config) throws Exception {
			}

			@Override
			public String sqlQuery(String tablespace, long version, int partition, String query) throws DNodeException {
				return null;
			}

			@Override
			public String deploy(List<DeployAction> deployActions, long version) throws DNodeException {
				Assert.assertEquals(1, deployActions.size());
				Assert.assertEquals("hdfs://foo/bar/0.db", deployActions.get(0).getDataURI());
				Assert.assertEquals("partition1", deployActions.get(0).getTablespace());
				Assert.assertTrue(version >= 0); // TODO Is this the right checking here?
				return "FOO";
			}

			@Override
			public String rollback(List<RollbackAction> rollbackActions, String ignoreMe) throws DNodeException {
				return null;
			}

			@Override
			public String status() throws DNodeException {
				return null;
			}

			@Override
			public void stop() throws Exception {
			}

			@Override
			public void giveGreenLigth() {
			}

			@Override
			public String abortDeploy(long version) throws DNodeException {
				return null;
			}

			@Override
			public String deleteOldVersions(List<com.splout.db.thrift.TablespaceVersion> versions) throws DNodeException {
				return null;
			}

			@Override
      public String testCommand(String command) throws DNodeException {
	      // TODO Auto-generated method stub
	      return null;
      }
		}, "dnode-" + this.getClass().getName() + "-5");

		try {
			handler.init(config);

			ReplicationEntry repEntry = new ReplicationEntry(0, dnode.getAddress());

			DeployRequest deployRequest = new DeployRequest();
			deployRequest.setTablespace("partition1");
			deployRequest.setPartitionMap(PartitionMap.oneShardOpenedMap().getPartitionEntries());
			deployRequest.setReplicationMap(Arrays.asList(repEntry));
			deployRequest.setData_uri("hdfs://foo/bar");

			List<DeployRequest> l = new ArrayList<DeployRequest>();
			l.add(deployRequest);
			handler.deploy(l);
		} finally {
			handler.close();
			dnode.stop();
			Hazelcast.shutdownAll();
		}
	}

	@Test
	public void testInitDNodeList() throws Throwable {
		SploutConfiguration config = SploutConfiguration.getTestConfig();
		QNodeHandler handler = new QNodeHandler();
		try {
			HazelcastInstance hz = Hazelcast.newHazelcastInstance(HazelcastConfigBuilder.build(config));
			CoordinationStructures coord = new CoordinationStructures(hz);

			SploutConfiguration dNodeConfig = SploutConfiguration.getTestConfig();
			dNodeConfig.setProperty(DNodeProperties.PORT, 1000);

			coord.getDNodes().put("/localhost:1000", new DNodeInfo(dNodeConfig));

			dNodeConfig = SploutConfiguration.getTestConfig();
			dNodeConfig.setProperty(DNodeProperties.PORT, 1001);

			coord.getDNodes().put("/localhost:1001", new DNodeInfo(dNodeConfig));

			handler.init(config);
			Assert.assertEquals(handler.getContext().getCoordinationStructures().getDNodes().values().size(), 2);
		} finally {
			handler.close();
			Hazelcast.shutdownAll();
		}
	}

	@Test
	public void testDNodeDownAndUp() throws Throwable {
		SploutConfiguration config = SploutConfiguration.getTestConfig();
		QNodeHandler handler = new QNodeHandler();
		HazelcastInstance hz = Hazelcast.newHazelcastInstance(HazelcastConfigBuilder.build(config));
		try {
			CoordinationStructures coord = new CoordinationStructures(hz);
			SploutConfiguration dNodeConfig = SploutConfiguration.getTestConfig();
			dNodeConfig.setProperty(DNodeProperties.PORT, 1000);
			coord.getDNodes().put("/localhost:1000", new DNodeInfo(dNodeConfig));

			handler.init(config);
			Assert.assertEquals(handler.getContext().getCoordinationStructures().getDNodes().values().size(), 1);

			coord.getDNodes().remove(coord.getDNodes().entrySet().iterator().next().getKey());

			Thread.sleep(100);
			Assert.assertEquals(handler.getContext().getCoordinationStructures().getDNodes().values().size(), 0);

			dNodeConfig = SploutConfiguration.getTestConfig();
			dNodeConfig.setProperty(DNodeProperties.PORT, 1001);
			coord.getDNodes().put("/localhost:1001", new DNodeInfo(dNodeConfig));
			Thread.sleep(100);

			Assert.assertEquals(handler.getContext().getCoordinationStructures().getDNodes().values().size(), 1);

			dNodeConfig = SploutConfiguration.getTestConfig();
			dNodeConfig.setProperty(DNodeProperties.PORT, 1000);
			coord.getDNodes().put("/localhost:1000", new DNodeInfo(dNodeConfig));
			Thread.sleep(100);

			Assert.assertEquals(handler.getContext().getCoordinationStructures().getDNodes().values().size(), 2);

			coord.getDNodes().remove(coord.getDNodes().entrySet().iterator().next().getKey());
			coord.getDNodes().remove(coord.getDNodes().entrySet().iterator().next().getKey());

			Thread.sleep(100);
			Assert.assertEquals(handler.getContext().getCoordinationStructures().getDNodes().values().size(), 0);
		} finally {
			handler.close();
			Hazelcast.shutdownAll();
		}
	}
}
