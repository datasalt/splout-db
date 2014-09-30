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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Test;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.splout.db.common.SploutConfiguration;
import com.splout.db.common.TestUtils;
import com.splout.db.hazelcast.CoordinationStructures;
import com.splout.db.hazelcast.DNodeInfo;
import com.splout.db.hazelcast.DistributedRegistry;
import com.splout.db.hazelcast.HazelcastConfigBuilder;

/**
 * Unit tests related to synchronization things with Hazelcast
 */
public class TestDNodeHZ {
  public static final int WAIT_AT_MOST = 15000;

	@After
	public void cleanUp() throws IOException {
		TestUtils.cleanUpTmpFolders(this.getClass().getName(), 4);
	}

	/**
	 * Test that the DNode receives "delete" events and it deletes the local storage
	 */
	@Test
	public void testHandleDeleteEventAfterCreation() throws Throwable {
		SploutConfiguration testConfig = SploutConfiguration.getTestConfig();
		DNode dnode = null;
		DNodeHandler handler = new DNodeHandler();
		File dataFolder = new File("dnode-" + this.getClass().getName() + "-1");
		try {
			// First we create the DNode
			dnode = TestUtils.getTestDNode(testConfig, handler, "dnode-" + this.getClass().getName() + "-1", true);

			// Let's say we have tablespace "t1" associated with version 2
			com.splout.db.thrift.TablespaceVersion tV = new com.splout.db.thrift.TablespaceVersion("t1", 2l);

			// It would be located in this folder:
			final File expectedFolder = handler.getLocalStorageFolder("t1", 0, 2l);
			expectedFolder.mkdirs();

			// Now we delete this version - so the DNode must also delete the local storage
			handler.deleteOldVersions(Arrays.asList(new com.splout.db.thrift.TablespaceVersion[] { tV }));
			
			new TestUtils.NotWaitingForeverCondition() {
				@Override
				public boolean endCondition() {
					return !expectedFolder.exists();
				}
			}.waitAtMost(WAIT_AT_MOST);

		} finally {
			if(dnode != null) {
				dnode.stop();
			}
			FileUtils.deleteDirectory(dataFolder);
			Hazelcast.shutdownAll();
		}
	}

	/*
	 * Test membership registering
	 */
	@Test
	public void testHZRegister() throws Throwable {
		SploutConfiguration testConfig = SploutConfiguration.getTestConfig();
		DNode dnode = null;
		try {
			final DNodeHandler handler = new DNodeHandler();
			dnode = TestUtils.getTestDNode(testConfig, handler, "dnode-" + this.getClass().getName() + "-4", true);

			// Assert that this DNode is registered for failover
			HazelcastInstance hz = Hazelcast.newHazelcastInstance(HazelcastConfigBuilder.build(testConfig));
			final CoordinationStructures coord = new CoordinationStructures(hz);
			// We need to implement the {@link com.splout.db.hazelcast.DistributedRegistry} for members to be
			// automatically removed from the DNODES map
			new DistributedRegistry(CoordinationStructures.DNODES, new DNodeInfo(testConfig), hz, 10, 3);

			new TestUtils.NotWaitingForeverCondition() {
				@Override
				public boolean endCondition() {
					boolean registered = false;
					for(DNodeInfo info: coord.getDNodes().values()) {
						if(info.getAddress().equals(handler.whoAmI())) {
							registered = true;
						}
					}
					return registered;
				}
			}.waitAtMost(WAIT_AT_MOST);

			dnode.stop();
			dnode = null;

			new TestUtils.NotWaitingForeverCondition() {
				@Override
				public boolean endCondition() {
					boolean unregistered = true;
					for(DNodeInfo info: coord.getDNodes().values()) {
						if(info.getAddress().equals(handler.whoAmI())) {
							unregistered = false;
						}
					}
					return unregistered;
				}
			}.waitAtMost(5000);

		} finally {
			if(dnode != null) {
				dnode.stop();
			}
			Hazelcast.shutdownAll();
		}
	}
}
