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

import java.util.List;

import com.splout.db.common.BaseBean;
import com.splout.db.common.PartitionEntry;
import com.splout.db.common.ReplicationEntry;

/**
 * Bean that is serialized as JSON and sent to the QNodes for requesting a deploy. The deploy method accepts a list of such beans. Each bean refers to the deplooyment of one tablespace.
 */
public class DeployRequest extends BaseBean {

	String tablespace;
	String data_uri;
	List<String> initStatements;
	List<PartitionEntry> partitionMap;
	List<ReplicationEntry> replicationMap;

	// ----------------- //
	public String getTablespace() {
		return tablespace;
	}

	public void setTablespace(String table_partition) {
		this.tablespace = table_partition;
	}

	public String getData_uri() {
		return data_uri;
	}

	public void setData_uri(String data_uri) {
		this.data_uri = data_uri;
	}

	public List<PartitionEntry> getPartitionMap() {
		return partitionMap;
	}

	public void setPartitionMap(List<PartitionEntry> partitionMap) {
		this.partitionMap = partitionMap;
	}

	public List<ReplicationEntry> getReplicationMap() {
		return replicationMap;
	}

	public void setReplicationMap(List<ReplicationEntry> replicationMap) {
		this.replicationMap = replicationMap;
	}

	public List<String> getInitStatements() {
  	return initStatements;
  }

	public void setInitStatements(List<String> initStatements) {
  	this.initStatements = initStatements;
  }
}