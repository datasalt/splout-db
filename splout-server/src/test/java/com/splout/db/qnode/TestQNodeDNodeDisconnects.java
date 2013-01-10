package com.splout.db.qnode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.splout.db.common.SploutConfiguration;
import com.splout.db.common.TestUtils;
import com.splout.db.dnode.DNode;
import com.splout.db.dnode.DNodeHandler;

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
			}.waitAtMost(5000);

			dnode1.stop();

			assertEquals(handler.getDNodeList().size(), 0);
			assertTrue(handler.getContext().getThriftClientCache().get(dnode1.getAddress()).isEmpty());

			dnode1 = TestUtils.getTestDNode(config, dNodeHandler1, "test-dnode-" + this.getClass().getName());

			assertEquals(handler.getDNodeList().size(), 1);

			// wait until connection pool has been regenerated
			new TestUtils.NotWaitingForeverCondition() {

				@Override
				public boolean endCondition() {
					return handler.getContext().getThriftClientCache().get(dnode1Address).size() == 40;
				}
			}.waitAtMost(5000);

		} finally {
			dnode1.stop();
			qnode.close();
		}
	}
}
