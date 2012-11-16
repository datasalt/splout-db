package com.splout.db.qnode.beans;

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

import java.util.Map;

import com.splout.db.common.Tablespace;
import com.splout.db.dnode.beans.DNodeSystemStatus;

/**
 * All the information available to the QNode including the alive DNodes, the partition map info, etc.
 */
public class QNodeStatus {

	private Map<String, DNodeSystemStatus> dNodes;
	private Map<String, Tablespace> tablespaceMap;
	private int clusterSize;
	
	public QNodeStatus() {
	}

	public Map<String, DNodeSystemStatus> getdNodes() {
  	return dNodes;
  }
	public void setdNodes(Map<String, DNodeSystemStatus> dNodes) {
  	this.dNodes = dNodes;
  }
	public Map<String, Tablespace> getTablespaceMap() {
  	return tablespaceMap;
  }
	public void setTablespaceMap(Map<String, Tablespace> tablespaceMap) {
  	this.tablespaceMap = tablespaceMap;
  }
	public int getClusterSize() {
  	return clusterSize;
  }
	public void setClusterSize(int clusterSize) {
  	this.clusterSize = clusterSize;
  }
}
