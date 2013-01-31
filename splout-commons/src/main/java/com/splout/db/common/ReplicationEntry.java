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
 * An entry in a {@link ReplicationMap}. It consists of a shard id and a list of nodes (addresses in the form host:port).
 * 
 * @see ReplicationMap
 */
@SuppressWarnings("serial")
public class ReplicationEntry extends BaseBean implements Serializable, Comparable<ReplicationEntry> {

	public String toString() {
		return "Shard:" + shard + ",nodes:" + nodes;
	}
	
	Integer shard;
	List<String> nodes;
	int expectedReplicationFactor;

	public ReplicationEntry() {
		
	}
	
	public ReplicationEntry(int shard, String... nodes) {
		this.shard = shard;
		this.nodes = new ArrayList<String>();
		this.nodes.addAll(Arrays.asList(nodes));
		this.expectedReplicationFactor = this.nodes.size();
	}
	
	// ----------------- //
	public Integer getShard() {
		return shard;
	}

	public void setShard(Integer shard) {
		this.shard = shard;
	}

	public List<String> getNodes() {
		return nodes;
	}

	public void setNodes(List<String> nodes) {
		this.nodes = nodes;
	}
	
	public void setExpectedReplicationFactor(int expectedReplicationFactor) {
  	this.expectedReplicationFactor = expectedReplicationFactor;
  }

	public int getExpectedReplicationFactor() {
  	return expectedReplicationFactor;
  }

	@Override
	public boolean equals(Object obj) {
		ReplicationEntry reEntry = (ReplicationEntry)obj;
		return shard.equals(reEntry.shard);
	}
	
	public int hashCode() {
		return shard.hashCode();
	}

	@Override
  public int compareTo(ReplicationEntry o) {
		return shard.compareTo(o.shard);
	}
}