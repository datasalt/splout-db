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

import com.splout.db.common.JSONSerDe.JSONSerDeException;
import com.splout.db.common.*;
import com.splout.db.dnode.DNode;
import com.splout.db.dnode.IDNodeHandler;
import com.splout.db.hazelcast.TablespaceVersion;
import com.splout.db.qnode.beans.ErrorQueryStatus;
import com.splout.db.qnode.beans.QueryStatus;
import com.splout.db.thrift.DNodeException;
import com.splout.db.thrift.DeployAction;
import com.splout.db.thrift.RollbackAction;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestQuerier {
	
	@AfterClass
	@BeforeClass
	public static void cleanUp() throws IOException {
		TestUtils.cleanUpTmpFolders(TestQuerier.class.getName(), 2);
	}
	
	@Test
	@Ignore
	public void testAllFailingDNodes() throws JSONSerDeException, Querier.QuerierException {
		SploutConfiguration testConfig = SploutConfiguration.getTestConfig();
		QNodeHandlerContext context = new QNodeHandlerContext(testConfig, null);

		List<ReplicationEntry> rEntries = new ArrayList<ReplicationEntry>();
		rEntries.add(new ReplicationEntry(0, "localhost:4444", "localhost:5555", "localhost:6666"));

		Tablespace tablespace = new Tablespace(PartitionMap.oneShardOpenedMap(), new ReplicationMap(rEntries), 0, 0);
		context.getTablespaceVersionsMap().put(new TablespaceVersion("t1", 0l), tablespace);
		context.getCurrentVersionsMap().put("t1", 0l);

		Querier querier = new Querier(context);
		
		// All DNodes are tried since they are alive and because all including the last one fail, the error is returned
		for(int i = 0; i < 3; i++) {
			ErrorQueryStatus status = (ErrorQueryStatus) querier.query("t1", "", 0);
			assertTrue(status.getError().contains("connecting to client localhost:6666"));
		}
	}
	
	@Test
	public void testRoundRobin() throws Throwable {
		SploutConfiguration testConfig = SploutConfiguration.getTestConfig();
		// A handler that returns OK to any query
		IDNodeHandler okQueryHandler = new IDNodeHandler() {
			@Override
      public void init(SploutConfiguration config) throws Exception {
      }
			@Override
      public void giveGreenLigth() {
      }
			@Override
      public String sqlQuery(String tablespace, long version, int partition, String query) throws DNodeException {
	      return "[{ \"msg\": \"OK\" }]";
      }
			@Override
      public String deploy(List<DeployAction> deployActions, long version) throws DNodeException {
	      return null;
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
		};
		DNode dnode1 = TestUtils.getTestDNode(testConfig, okQueryHandler, "dnode-" + this.getClass().getName() + "-1");
		DNode dnode2 = TestUtils.getTestDNode(testConfig, okQueryHandler, "dnode-" + this.getClass().getName() + "-2");
		
		List<ReplicationEntry> rEntries = new ArrayList<ReplicationEntry>();
		rEntries.add(new ReplicationEntry(0, dnode1.getAddress(), dnode2.getAddress()));

		QNodeHandlerContext context = new QNodeHandlerContext(testConfig, null);
		
		Tablespace tablespace = new Tablespace(PartitionMap.oneShardOpenedMap(), new ReplicationMap(rEntries), 0, 0);
		context.getTablespaceVersionsMap().put(new TablespaceVersion("t1", 0l), tablespace);
		context.getCurrentVersionsMap().put("t1", 0l);

		Querier querier = new Querier(context);
		
		/*
		 * Here we are testing basic round robin: we have 2 nodes serving one shard and we do 5 queries
		 * so we want to assert that the node whom we are querying at step "i" is exactly "i % 2".
		 */
		for(int i = 0; i < 5; i++) {
			QueryStatus status = querier.query("t1", "", 0);
			assertEquals((Integer)(i % 2), querier.getPartitionRoundRobin().get(0));
			assertEquals((Integer)0, status.getShard());
			assertEquals(null, status.getError());
		}
	}
	
	@Test
	public void testRoundRobinWithSomeDeadNodes() throws Throwable {
		SploutConfiguration testConfig = SploutConfiguration.getTestConfig();
		// A handler that returns OK to any query
		IDNodeHandler okQueryHandler = new IDNodeHandler() {
			@Override
      public void init(SploutConfiguration config) throws Exception {
      }
			@Override
      public void giveGreenLigth() {
      }
			@Override
      public String sqlQuery(String tablespace, long version, int partition, String query) throws DNodeException {
	      return "[{ \"msg\": \"OK\" }]";
      }
			@Override
      public String deploy(List<DeployAction> deployActions, long version) throws DNodeException {
	      return null;
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
		};
		DNode dnode1 = TestUtils.getTestDNode(testConfig, okQueryHandler, "dnode-" + this.getClass().getName() + "-1");
		DNode dnode2 = TestUtils.getTestDNode(testConfig, okQueryHandler, "dnode-" + this.getClass().getName() + "-2");
		
		List<ReplicationEntry> rEntries = new ArrayList<ReplicationEntry>();
		rEntries.add(new ReplicationEntry(0, dnode1.getAddress(), "fakeaddress:1111", dnode2.getAddress(), "fakeaddress:2222"));

		QNodeHandlerContext context = new QNodeHandlerContext(testConfig, null);
		
		Tablespace tablespace = new Tablespace(PartitionMap.oneShardOpenedMap(), new ReplicationMap(rEntries), 0, 0);
		context.getTablespaceVersionsMap().put(new TablespaceVersion("t1", 0l), tablespace);
		context.getCurrentVersionsMap().put("t1", 0l);

		Querier querier = new Querier(context);
		
		/*
		 * Here we are testing round robin when there are some dead nodes in a shard. We have 2 alive nodes and 2 dead nodes.
		 * Therefore we want to assert that at step "i" the chosen node will be "0 if i % 2 == 0 or 2 if i % 2 == 1".
		 * This can be simplified to (i % 2) * 2
		 */
		for(int i = 0; i < 7; i++) {
			QueryStatus status = querier.query("t1", "", 0);
			assertEquals((Integer)((i % 2) * 2), querier.getPartitionRoundRobin().get(0));
			assertEquals((Integer)0, status.getShard());
			assertEquals(null, status.getError());
		}
	}

	@Test
	public void testRoundRobinWithSomeFailingNodes() throws Throwable {
		SploutConfiguration testConfig = SploutConfiguration.getTestConfig();
		// A handler that returns OK to any query
		IDNodeHandler okQueryHandler = new IDNodeHandler() {
			@Override
      public void init(SploutConfiguration config) throws Exception {
      }
			@Override
      public void giveGreenLigth() {
      }
			@Override
      public String sqlQuery(String tablespace, long version, int partition, String query) throws DNodeException {
	      return "[{ \"msg\": \"OK\" }]";
      }
			@Override
      public String deploy(List<DeployAction> deployActions, long version) throws DNodeException {
	      return null;
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
		};
		DNode dnode1 = TestUtils.getTestDNode(testConfig, okQueryHandler, "dnode-" + this.getClass().getName() + "-1");
		DNode dnode2 = TestUtils.getTestDNode(testConfig, okQueryHandler, "dnode-" + this.getClass().getName() + "-2");
		
		List<ReplicationEntry> rEntries = new ArrayList<ReplicationEntry>();
		rEntries.add(new ReplicationEntry(0, dnode1.getAddress(), "failingaddress:1111", dnode2.getAddress(), "failingaddress:2222"));

		QNodeHandlerContext context = new QNodeHandlerContext(testConfig, null);
		
		Tablespace tablespace = new Tablespace(PartitionMap.oneShardOpenedMap(), new ReplicationMap(rEntries), 0, 0);
		context.getTablespaceVersionsMap().put(new TablespaceVersion("t1", 0l), tablespace);
		context.getCurrentVersionsMap().put("t1", 0l);

		Querier querier = new Querier(context);
		
		/*
		 * Here we are testing round robin when there are failing nodes in a shard. We have 4 alive nodes with 2 failing nodes.
		 * The assertion is the same than in the case of some dead nodes but we do another unit test because the business logic
		 * inside the querier is different and we might have a bug anyway.
		 */
		for(int i = 0; i < 7; i++) {
			QueryStatus status = querier.query("t1", "", 0);
			assertEquals((Integer)((i % 2) * 2), querier.getPartitionRoundRobin().get(0));
			assertEquals((Integer)0, status.getShard());
			assertEquals(null, status.getError());
		}
	}
}
