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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A Splout's replication map definition.
 * <p>
 * A replication map is made up by a list of {@link ReplicationEntry}. Each of them specifies the list of addresses
 * (nodes) that hold the data for a shard. It is understood that if one {@link ReplicationEntry} has more than one node
 * then they will all hold the same data.
 */
@SuppressWarnings("serial")
public class ReplicationMap implements Serializable {

	public String toString() {
		return replicationEntries.toString();
	}
	
	List<ReplicationEntry> replicationEntries;

	public ReplicationMap() {
	}

	public ReplicationMap(List<ReplicationEntry> replicationMap) {
		this.replicationEntries = replicationMap;
	}

	public static ReplicationMap roundRobinMap(int nPartitions, int repFactor, String... hosts) {
		List<ReplicationEntry> repEntries = new ArrayList<ReplicationEntry>();
		for(int i = 0; i < nPartitions; i++) {
			int toNode = i % hosts.length;
			ReplicationEntry repEntry = new ReplicationEntry();
			repEntry.setShard(i);
			List<String> shardDNodes = new ArrayList<String>();
			for(int j = 0; j < repFactor; j++) {
				int chosen = (toNode + j) % hosts.length;
				shardDNodes.add(hosts[chosen]);
			}
			repEntry.setNodes(shardDNodes);
			repEntries.add(repEntry);
		}
		return new ReplicationMap(repEntries);
	}
	
	/**
	 * Returns a one to one replication map with consecutive shard Ids from a list of hosts. This method is useful for
	 * quickly having an instance of ReplicationMap that has no replication (1 shard -> 1 host).
	 */
	public static ReplicationMap oneToOneMap(String... hosts) {
		List<ReplicationEntry> replicationMap = new ArrayList<ReplicationEntry>();
		int shard = 0;
		for(String host : hosts) {
			ReplicationEntry entry = new ReplicationEntry();
			entry.setNodes(Arrays.asList(new String[] { host }));
			entry.setShard(shard);
			replicationMap.add(entry);
			shard++;
		}
		return new ReplicationMap(replicationMap);
	}

	public List<ReplicationEntry> getReplicationEntries() {
		return replicationEntries;
	}

	public void setReplicationEntries(List<ReplicationEntry> replicationEntries) {
		this.replicationEntries = replicationEntries;
	}
}
