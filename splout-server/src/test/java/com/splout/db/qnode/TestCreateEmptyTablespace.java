package com.splout.db.qnode;

import java.io.IOException;
import java.util.Arrays;

import org.junit.After;
import org.junit.Test;

import com.hazelcast.core.Hazelcast;
import com.splout.db.common.ReplicationEntry;
import com.splout.db.common.SploutConfiguration;
import com.splout.db.common.TestUtils;
import com.splout.db.dnode.DNode;
import com.splout.db.dnode.DNodeHandler;
import com.splout.db.qnode.beans.DeployRequest;

public class TestCreateEmptyTablespace {

	@After
	public void cleanUp() throws IOException {
		TestUtils.cleanUpTmpFolders(this.getClass().getName(), 2);
	}
	
	@Test
	public void testDeployFinalizing() throws Throwable {
		final QNodeHandler handler = new QNodeHandler();
		SploutConfiguration config = SploutConfiguration.getTestConfig();
		SploutConfiguration config1 = SploutConfiguration.getTestConfig();
		SploutConfiguration config2 = SploutConfiguration.getTestConfig();
		DNodeHandler dHandler1 = new DNodeHandler();
		DNode dnode1 = TestUtils.getTestDNode(config1, dHandler1, "dnode-" + this.getClass().getName() + "-1");
		DNodeHandler dHandler2 = new DNodeHandler();
		DNode dnode2 = TestUtils.getTestDNode(config2, dHandler2, "dnode-" + this.getClass().getName() + "-2");

		try {
			handler.init(config);

			ReplicationEntry repEntry1 = new ReplicationEntry(0,  dnode1.getAddress(), dnode2.getAddress());

			DeployRequest deployRequest1 = new DeployRequest();
			deployRequest1.setTablespace("emptyTablespace");
			deployRequest1.setReplicationMap(Arrays.asList(repEntry1));

			handler.createTablespace(deployRequest1);

			new TestUtils.NotWaitingForeverCondition() {
				
				@Override
				public boolean endCondition() {
					return handler.getContext().getTablespaceVersionsMap().size() == 1;
				}
			};

			// everything OK
		} finally {
			handler.close();
			dnode1.stop();
			dnode2.stop();
			Hazelcast.shutdownAll();
		}
	}
}
