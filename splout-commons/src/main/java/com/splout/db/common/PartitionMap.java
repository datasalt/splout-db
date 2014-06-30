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
import java.util.List;

import org.codehaus.jackson.type.TypeReference;

/**
 * A Splout's partition map definition - The generic type is the type of the Key. For example, if a tablespace is partitioned by an
 * Integer then T = Integer.
 * <p>
 * A partition map is composed by several {@link PartitionEntry}. The partition map implements the logic of finding a
 * partition number given a key of type <T> in {@link #findPartition(String)}.
 */
@SuppressWarnings("serial")
public class PartitionMap implements Serializable {

	public String toString() {
		return partitionEntries.toString();
	}
	
	public final static TypeReference<PartitionMap> PARTITION_MAP_REF = new TypeReference<PartitionMap>() {
	};

	// When a key can't be found in any partition.
	// An opened question is whether this shouldn't be contemplated and an exception should be thrown instead
	public static int NO_PARTITION = -1;

	private List<PartitionEntry> partitionEntries;
	
	public PartitionMap() {
	}

	/**
	 * Use this method for creating a PartitionMap that has only one shard (0) and whose range is -Infinity, +Infinity
	 */
	public static PartitionMap oneShardOpenedMap() {
		// For convenience, we create a 1-shard opened map here (shard = 0)
		List<PartitionEntry> partitionMap = new ArrayList<PartitionEntry>();
		PartitionEntry openedEntry = new PartitionEntry();
		openedEntry.setMin(null);
		openedEntry.setMax(null);
		openedEntry.setShard(0);
		partitionMap.add(openedEntry);
		return new PartitionMap(partitionMap);
	}

	public PartitionMap(List<PartitionEntry> partitionMap) {
		if(partitionMap == null || partitionMap.size() == 0) {
			throw new IllegalArgumentException("Can't create an empty / null partition map. For an opened 1-shard map with shard id = 0 use default constructor.");
		}
		this.partitionEntries = partitionMap;
	}

	/**
	 * Given a min and a max key, return all the partitions that impact this range, min and max inclusive.
	 * <p>
	 * Note that (null, null) is a valid input to this method and will be interpreted as the whole key range, regardless
	 * of the key type (that's why we use null for representing opened ranges).
	 */
	public List<Integer> findPartitions(String minKey, String maxKey) {
		List<Integer> partitions = new ArrayList<Integer>();
		int minPartitionIndex = 0, maxPartitionIndex = partitionEntries.size() - 1;
		if(minKey != null) {
			minPartitionIndex = findPartition(minKey);
		}
		if(maxKey != null) {
			maxPartitionIndex = findPartition(maxKey);
		}
		for(int i = minPartitionIndex; i <= maxPartitionIndex; i++) {
			partitions.add(partitionEntries.get(i).getShard());
		}
		return partitions;
	}

	/**
	 * Given a key, return the partition that it belongs to, or {@link #NO_PARTITION} if none matches. Mathematically,
	 * partitions match (min, max].
	 */
	public int findPartition(String keyObj) {
		if(keyObj == null) {
			throw new IllegalArgumentException("Key obj can't be null for findPartition()");
		}
		for(PartitionEntry entry : partitionEntries) {
			// We assume (-Infinity, Infinity) matching since nulls represent Infinity for any type <T>
			boolean minMatches = true;
			boolean maxMatches = true;
			if(entry.getMin() != null) {
				if((entry.getMin()).compareTo(keyObj) >= 0) {
					minMatches = false;
				}
			}
			if(entry.getMax() != null) {
				if((entry.getMax()).compareTo(keyObj) < 0) {
					maxMatches = false;
				}
			}
			if(minMatches && maxMatches) {
				return entry.getShard();
			}
		}
		return NO_PARTITION;
	}

	public void setPartitionEntries(List<PartitionEntry> partitionEntries) {
		this.partitionEntries = partitionEntries;
	}

	public List<PartitionEntry> getPartitionEntries() {
		return partitionEntries;
	}
}
