package com.splout.db.qnode;

/*
 * #%L
 * Splout SQL Server
 * %%
 * Copyright (C) 2012 - 2013 Datasalt Systems S.L.
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
