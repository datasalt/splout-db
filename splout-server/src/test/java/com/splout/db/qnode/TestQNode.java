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
import com.splout.db.common.SploutConfiguration;
import com.splout.db.common.TestUtils;
import com.splout.db.qnode.beans.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestQNode {

  @Test
  public void test() throws Throwable {
    final SploutConfiguration config = SploutConfiguration.getTestConfig();
    QNode qnode = TestUtils.getTestQNode(config, new QNodeMockHandler() {
      @Override
      public QueryStatus query(String tablespace, String key, String sql, String partition, Integer cursorId) throws Exception {
        return new QueryStatus();
      }

      @Override
      public ArrayList<QueryStatus> multiQuery(String tablespace, List<String> keyMins, List<String> keyMaxs, String sql) throws Exception {
        return new ArrayList<QueryStatus>();
      }

      @Override
      public DeployInfo deploy(List<DeployRequest> deployRequest) throws Exception {
        return new DeployInfo();
      }

      @Override
      public StatusMessage rollback(List<SwitchVersionRequest> rollbackRequest) throws Exception {
        return new StatusMessage();
      }
    });

    qnode.close();
    Hazelcast.shutdownAll();
  }
}
