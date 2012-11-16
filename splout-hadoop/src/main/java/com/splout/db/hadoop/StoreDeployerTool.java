package com.splout.db.hadoop;

/*
 * #%L
 * Splout SQL Hadoop library
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datasalt.pangool.utils.HadoopUtils;
import com.splout.db.common.JSONSerDe;
import com.splout.db.common.JSONSerDe.JSONSerDeException;
import com.splout.db.common.PartitionMap;
import com.splout.db.common.ReplicationMap;
import com.splout.db.common.SploutClient;
import com.splout.db.qnode.beans.DeployInfo;
import com.splout.db.qnode.beans.DeployRequest;

/**
 * A generic class for deploying an already generated store by {@link TablespaceViewGenerator}.
 */
public class StoreDeployerTool {

	private final static Log log = LogFactory.getLog(StoreDeployerTool.class);

	private String qnode;
	private Configuration conf;

	public StoreDeployerTool(String qnode, Configuration conf) {
		this.qnode = qnode;
		this.conf = conf;
	}
	
	/**
	 * Deploy already generated tablespaces at a time. 
	 */
	@SuppressWarnings("unchecked")
  public void deploy(Collection<TablespaceDepSpec> deployments) throws JSONSerDeException, IOException {
		
		// We now query for the alive DNodes and build deployRequests accordingly
		DeployRequest[] deployRequests = new DeployRequest[deployments.size()];

		log.info("Querying Splout QNode for list of DNodes...");
		SploutClient client = new SploutClient(qnode);
		List<String> dnodes = client.dNodeList();		
		
		int tIndex = 0;
		for(TablespaceDepSpec tablespace : deployments) {
			Path tablespaceOut = new Path(tablespace.getSourcePath());

			// Define a DeployRequest for this Tablespace
			deployRequests[tIndex] = new DeployRequest();
						
			// Splout only accepts absolute URIs
			FileSystem sourceFs = tablespaceOut.getFileSystem(conf);
			if(!sourceFs.exists(tablespaceOut)) {
				throw new IllegalArgumentException("Folder doesn't exist: " + tablespaceOut);
			}
			Path absoluteOutPath = tablespaceOut.makeQualified(sourceFs);
			
			Path partitionMapPath = new Path(tablespaceOut, TablespaceGenerator.OUT_PARTITION_MAP);
			if(!sourceFs.exists(partitionMapPath)) {
				throw new IllegalArgumentException("Invalid tablespace folder: " + tablespaceOut + " doesn't contain a partition-map file.");
			}
			
			// Load the partition map
			PartitionMap partitionMap = JSONSerDe.deSer(
			    HadoopUtils.fileToString(sourceFs, partitionMapPath), PartitionMap.class);
			    			
			// Load the init statements, if they exist
			Path initStatementsPath = new Path(tablespaceOut, TablespaceGenerator.OUT_INIT_STATEMENTS);
			if(sourceFs.exists(initStatementsPath)) {
				List<String> initStatements = JSONSerDe.deSer(HadoopUtils.fileToString(sourceFs, initStatementsPath), ArrayList.class);
				deployRequests[tIndex].setInitStatements(initStatements);
			}
			
			deployRequests[tIndex].setTablespace(tablespace.getTablespace());
			deployRequests[tIndex].setData_uri(new Path(absoluteOutPath, "store").toUri().toString());
			deployRequests[tIndex].setPartitionMap(partitionMap.getPartitionEntries());
			
			// If rep>dnodes, imposible to reach this level of replication
			int repFactor = tablespace.getReplication();
			if (dnodes.size()<repFactor) {
				System.out.println("WARNING: Replication factor " + repFactor +" for tablespace " + tablespace.getTablespace() + " is bigger than the number of serving DNodes. Adjusting replication factor to "+dnodes.size());
				repFactor = dnodes.size();
			} 

			deployRequests[tIndex].setReplicationMap(ReplicationMap.roundRobinMap(partitionMap.getPartitionEntries().size(), repFactor,
			    dnodes.toArray(new String[0])).getReplicationEntries());
			
			tIndex++;
		}
		
		// Finally we send the deploy request
		DeployInfo dInfo = client.deploy(deployRequests);

		log.info("Deploy request of [" + deployments.size() + "] tablespaces performed. Deploy on [" + qnode + "] with version [" + dInfo.getVersion() + "] in progress.");
	}
	
}
