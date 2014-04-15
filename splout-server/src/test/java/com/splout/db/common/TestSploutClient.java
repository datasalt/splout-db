package com.splout.db.common;

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

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;

import com.splout.db.qnode.IQNodeHandler;
import com.splout.db.qnode.QNode;
import com.splout.db.qnode.QNodeMockHandler;
import com.splout.db.qnode.beans.DeployInfo;
import com.splout.db.qnode.beans.DeployRequest;
import com.splout.db.qnode.beans.QueryStatus;

public class TestSploutClient {

	public static IQNodeHandler QNODE_HANDLER = new QNodeMockHandler() {

		@Override
	  public QueryStatus query(String tablespace, String key, String sql, String partition) throws Exception {
		  QueryStatus queryStatus = new QueryStatus();
		  queryStatus.setError(tablespace + " " + key + " " + sql);
		  return queryStatus;
	  }
		@Override
	  public DeployInfo deploy(List<DeployRequest> deployReq) throws Exception {
		  return new DeployInfo("ok");
	  }
		@Override
	  public List<String> getDNodeList() throws Exception {
		  return new ArrayList<String>(Arrays.asList(new String[] { "ok" }));
	  }
	};
	
	@Test
	public void test() throws Exception {
		QNode qnode = new QNode();
		try {
			qnode.start(SploutConfiguration.getTestConfig(), QNODE_HANDLER);
			
			SploutClient client = new SploutClient(qnode.getAddress());
			
			QueryStatus queryStatus = client.query("t1", "k1", "SELECT * FROM foo;", null);
			assertEquals("t1 k1 SELECT * FROM foo;", queryStatus.getError());
			
			List<String> dnodes = client.dNodeList();
			assertEquals("ok", dnodes.get(0));

			DeployInfo info = client.deploy("t1", PartitionMap.oneShardOpenedMap(), ReplicationMap.oneToOneMap("http://localhost:4444"), new URI("file:///foo"));
			assertEquals("ok", info.getError());
			
		} finally {
			qnode.close();
		}
	}
	
	@Test
	@Ignore
	public void testPost() throws Exception {
		QNode qnode = new QNode();
		try {
			qnode.start(SploutConfiguration.getTestConfig(), QNODE_HANDLER);
			
			SploutClient client = new SploutClient(qnode.getAddress());
			
			QueryStatus queryStatus = client.queryPost("t1", "k1", "SELECT * FROM foo;", null);
			assertEquals("t1 k1 SELECT * FROM foo;", queryStatus.getError());
			
			List<String> dnodes = client.dNodeList();
			assertEquals("ok", dnodes.get(0));

			DeployInfo info = client.deploy("t1", PartitionMap.oneShardOpenedMap(), ReplicationMap.oneToOneMap("http://localhost:4444"), new URI("file:///foo"));
			assertEquals("ok", info.getError());
			
		} finally {
			qnode.close();
		}
	}

}
