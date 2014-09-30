package com.splout.db.dnode;

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

import java.util.List;

import org.junit.Test;

import com.splout.db.common.SploutConfiguration;
import com.splout.db.common.TestUtils;
import com.splout.db.qnode.QNode;
import com.splout.db.qnode.QNodeHandler;

public class TestDNodeTestAPI {

  public static final int WAIT_AT_MOST = 25000;

	@Test
	public void test() throws Exception {
		SploutConfiguration testConfig = SploutConfiguration.getTestConfig();
		testConfig.setProperty(DNodeProperties.HANDLE_TEST_COMMANDS, true);
		final DNode dnode = new DNode(testConfig, new DNodeHandler());
		dnode.init();
		
		final QNode qnode = new QNode();
		qnode.start(testConfig, new QNodeHandler());

		// Wait until the QNode has 1 Dnode in the list
		waitUntilThereAreThatManyDNodes(qnode, 1);
		
		dnode.testCommand(TestCommands.SHUTDOWN.toString());
		
		// Wait until the QNode has 0 DNodes in the list
		waitUntilThereAreThatManyDNodes(qnode, 0);

		dnode.testCommand(TestCommands.RESTART.toString());
		
		// Wait until the QNode has 1 Dnode in the list
		waitUntilThereAreThatManyDNodes(qnode, 1);
	}
	
	private void waitUntilThereAreThatManyDNodes(final QNode qnode, final int nDnodes) throws InterruptedException {
		// Wait until the QNode has 1 Dnode in the list
		new TestUtils.NotWaitingForeverCondition() {
			
			@Override
			public boolean endCondition() {
				try {
	        List<String> l = qnode.getHandler().getDNodeList();
	        return l.size() == nDnodes;
        } catch(Exception e) {
	        throw new RuntimeException(e);
        }
			}
		}.waitAtMost(WAIT_AT_MOST);
	}
}
