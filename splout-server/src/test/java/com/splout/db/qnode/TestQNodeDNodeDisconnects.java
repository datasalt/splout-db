package com.splout.db.qnode;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import com.splout.db.common.SploutConfiguration;
import com.splout.db.common.TestUtils;
import com.splout.db.dnode.DNode;
import com.splout.db.dnode.DNodeHandler;

public class TestQNodeDNodeDisconnects {

	@After
	@Before
	public void cleanUp() throws IOException {
		TestUtils.cleanUpTmpFolders(this.getClass().getName(), 4);
	}
	
	@Test
	public void test() throws Throwable {
		final SploutConfiguration config = SploutConfiguration.getTestConfig();
		QNodeHandler handler = new QNodeHandler();
		QNode qnode = TestUtils.getTestQNode(config, handler); 
		
		DNodeHandler dNodeHandler = new DNodeHandler();
		DNode dnode = TestUtils.getTestDNode(config, dNodeHandler, "dnode-" + this.getClass().getName());
		
		try {
			
			assertEquals(handler.getDNodeList().size(), 1);
			
			dnode.stop();
			
			assertEquals(handler.getDNodeList().size(), 0);
			
			dnode = TestUtils.getTestDNode(config, dNodeHandler, "test-dnode-" + this.getClass().getName());
			
			assertEquals(handler.getDNodeList().size(), 1);
			
		} finally {
			dnode.stop();
			qnode.close();
		}
	}
}
