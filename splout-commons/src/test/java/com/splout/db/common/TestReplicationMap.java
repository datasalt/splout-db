package com.splout.db.common;

/*
 * #%L
 * Splout SQL commons
 * %%
 * Copyright (C) 2012 Datasalt Systems S.L.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestReplicationMap {

	/**
	 * Through this example one can understand how is a "Round-robin replication map" built. Each shard (partition) is
	 * assigned to consecutive hosts up to the specified replication factor. Each new shard is assigned starting from the
	 * next host.
	 */
	@Test
	public void testRoundRobinMap() {
		ReplicationMap rMap = ReplicationMap.roundRobinMap(6, 2, "host1", "host2", "host3");
		assertEquals(6, rMap.getReplicationEntries().size());

		assertEquals(0, (int) rMap.getReplicationEntries().get(0).getShard());
		assertEquals(2, (int) rMap.getReplicationEntries().get(0).getNodes().size());
		assertEquals("host1", rMap.getReplicationEntries().get(0).getNodes().get(0));
		assertEquals("host2", rMap.getReplicationEntries().get(0).getNodes().get(1));

		assertEquals(1, (int) rMap.getReplicationEntries().get(1).getShard());
		assertEquals(2, (int) rMap.getReplicationEntries().get(1).getNodes().size());
		assertEquals("host2", rMap.getReplicationEntries().get(1).getNodes().get(0));
		assertEquals("host3", rMap.getReplicationEntries().get(1).getNodes().get(1));

		assertEquals(2, (int) rMap.getReplicationEntries().get(2).getShard());
		assertEquals(2, (int) rMap.getReplicationEntries().get(2).getNodes().size());
		assertEquals("host3", rMap.getReplicationEntries().get(2).getNodes().get(0));
		assertEquals("host1", rMap.getReplicationEntries().get(2).getNodes().get(1));

		assertEquals(3, (int) rMap.getReplicationEntries().get(3).getShard());
		assertEquals(2, (int) rMap.getReplicationEntries().get(3).getNodes().size());
		assertEquals("host1", rMap.getReplicationEntries().get(3).getNodes().get(0));
		assertEquals("host2", rMap.getReplicationEntries().get(3).getNodes().get(1));

		assertEquals(4, (int) rMap.getReplicationEntries().get(4).getShard());
		assertEquals(2, (int) rMap.getReplicationEntries().get(4).getNodes().size());
		assertEquals("host2", rMap.getReplicationEntries().get(4).getNodes().get(0));
		assertEquals("host3", rMap.getReplicationEntries().get(4).getNodes().get(1));

		assertEquals(5, (int) rMap.getReplicationEntries().get(5).getShard());
		assertEquals(2, (int) rMap.getReplicationEntries().get(5).getNodes().size());
		assertEquals("host3", rMap.getReplicationEntries().get(5).getNodes().get(0));
		assertEquals("host1", rMap.getReplicationEntries().get(5).getNodes().get(1));
	}

	@Test
	public void test() {
		ReplicationMap rMap = ReplicationMap.roundRobinMap(3, 3, "host1", "host2", "host3");
		assertEquals(3, rMap.getReplicationEntries().size());

		assertEquals(0, (int) rMap.getReplicationEntries().get(0).getShard());
		assertEquals(3, (int) rMap.getReplicationEntries().get(0).getNodes().size());
		assertEquals("host1", rMap.getReplicationEntries().get(0).getNodes().get(0));
		assertEquals("host2", rMap.getReplicationEntries().get(0).getNodes().get(1));
		assertEquals("host3", rMap.getReplicationEntries().get(0).getNodes().get(2));
		
		assertEquals(1, (int) rMap.getReplicationEntries().get(1).getShard());
		assertEquals(3, (int) rMap.getReplicationEntries().get(1).getNodes().size());
		assertEquals("host2", rMap.getReplicationEntries().get(1).getNodes().get(0));
		assertEquals("host3", rMap.getReplicationEntries().get(1).getNodes().get(1));
		assertEquals("host1", rMap.getReplicationEntries().get(1).getNodes().get(2));

		assertEquals(2, (int) rMap.getReplicationEntries().get(2).getShard());
		assertEquals(3, (int) rMap.getReplicationEntries().get(2).getNodes().size());
		assertEquals("host3", rMap.getReplicationEntries().get(2).getNodes().get(0));
		assertEquals("host1", rMap.getReplicationEntries().get(2).getNodes().get(1));
		assertEquals("host2", rMap.getReplicationEntries().get(2).getNodes().get(2));
	}
}
