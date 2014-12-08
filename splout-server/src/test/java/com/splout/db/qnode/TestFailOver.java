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
import com.splout.db.common.*;
import com.splout.db.dnode.DNode;
import com.splout.db.dnode.DNodeMockHandler;
import com.splout.db.dnode.DNodeProperties;
import com.splout.db.dnode.IDNodeHandler;
import com.splout.db.hazelcast.TablespaceVersion;
import com.splout.db.qnode.beans.QueryStatus;
import com.splout.db.thrift.DNodeException;
import com.splout.db.thrift.DeployAction;
import com.splout.db.thrift.RollbackAction;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class TestFailOver {

  @After
  public void cleanUp() throws IOException {
    TestUtils.cleanUpTmpFolders(this.getClass().getName(), 2);
  }

  /*
   * The mock DHandler that fails
   */
  IDNodeHandler failingDHandler = new DNodeMockHandler() {

    @Override
    public String sqlQuery(String tablespace, long version, int partition, String query) throws DNodeException {
      throw new DNodeException(0, "I always fail");
    }

    @Override
    public String deploy(List<DeployAction> deployActions, long version) throws DNodeException {
      throw new DNodeException(0, "I always fail");
    }

    @Override
    public String rollback(List<RollbackAction> rollbackActions, String ignoreMe) throws DNodeException {
      throw new DNodeException(0, "I always fail");
    }

    @Override
    public String status() throws DNodeException {
      throw new DNodeException(0, "I always fail");
    }
  };

  /*
   * The mock DHandler that doesn't fail
   */
  DNodeMockHandler dHandler = new DNodeMockHandler() {

    @Override
    public String sqlQuery(String tablespace, long version, int partition, String query) throws DNodeException {
      return "[1]";
    }

    @Override
    public String deploy(List<DeployAction> deployActions, long distributedBarrier) throws DNodeException {
      return "FOO";
    }

    @Override
    public String rollback(List<RollbackAction> rollbackActions, String ignoreMe) throws DNodeException {
      return "FOO";
    }

    public String status() throws DNodeException {
      return "FOO";
    }
  };

  @Test
	public void testQuery() throws Throwable {
		QNodeHandler handler = new QNodeHandler();
		handler.init(SploutConfiguration.getTestConfig());
		
		SploutConfiguration config1 = SploutConfiguration.getTestConfig();
		SploutConfiguration config2 = SploutConfiguration.getTestConfig();
		config2.setProperty(DNodeProperties.DATA_FOLDER, config1.getString(DNodeProperties.DATA_FOLDER) + "-" + 1);
		
		DNode dnode1 = TestUtils.getTestDNode(config1, failingDHandler, "dnode-" + this.getClass().getName() + "-1");
		DNode dnode2 = TestUtils.getTestDNode(config2, dHandler, "dnode-" + this.getClass().getName() + "-2");

		try {
			ReplicationEntry repEntry = new ReplicationEntry(0, dnode1.getAddress(), dnode2.getAddress());
			Tablespace tablespace1 = new Tablespace(PartitionMap.oneShardOpenedMap(), new ReplicationMap(Arrays.asList(repEntry)), 1l, 0l);
			handler.getContext().getTablespaceVersionsMap().put(new TablespaceVersion("tablespace1", 1l), tablespace1);
			handler.getContext().getCurrentVersionsMap().put("tablespace1", 1l);

			QueryStatus qStatus = handler.query("tablespace1", "2", "SELECT 1;", null);
			Assert.assertEquals(new Integer(0), qStatus.getShard());
			Assert.assertEquals("[1]", qStatus.getResult().toString());
		} finally {
			handler.close();
			dnode1.stop();
			dnode2.stop();
			Hazelcast.shutdownAll();
		}
	}

}
