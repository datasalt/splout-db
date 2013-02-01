package com.splout.db.qnode;

import java.util.Set;

import com.hazelcast.impl.MProxyImpl;

/**
 * A proxy for Hazelcast's maps for mocking calls to {@link CoordinationStructures.#getDNodes()}
 * - only works if the caller calls "keySet()" ! 
 */
@SuppressWarnings("serial")
public class FixedDNodeList extends MProxyImpl {

	Set<String> dNodes;

	public FixedDNodeList(Set<String> dNodes) {
		this.dNodes = dNodes;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Set keySet() {
		return dNodes;
	}
}
