package com.splout.db.qnode;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.hazelcast.impl.MProxyImpl;
import com.splout.db.hazelcast.DNodeInfo;

/**
 * A proxy for Hazelcast's maps for mocking calls to {@link CoordinationStructures.#getDNodes()}
 * - only works if the caller calls "keySet()" ! 
 */
@SuppressWarnings("serial")
public class FixedDNodeList extends MProxyImpl {

	Set<String> dNodes;
	List<DNodeInfo> values;
	
	public FixedDNodeList(Set<String> dNodes, List<DNodeInfo> values) {
		this.dNodes = dNodes;
		this.values = values;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Set keySet() {
		return dNodes;
	}
	
	@SuppressWarnings("rawtypes")
  @Override
	public Collection values() {
		return values;
	}
}
