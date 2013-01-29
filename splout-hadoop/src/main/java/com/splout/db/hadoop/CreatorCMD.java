package com.splout.db.hadoop;

/*
 * #%L
 * Splout SQL Hadoop library
 * %%
 * Copyright (C) 2012 - 2013 Datasalt Systems S.L.
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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.splout.db.common.ReplicationMap;
import com.splout.db.common.SploutClient;
import com.splout.db.qnode.beans.DeployInfo;

/**
 * A simple tool for creating empty tablespaces. Can be called through hadoop jar ...
 */
public class CreatorCMD implements Tool {

	@Parameter(names = { "-r", "--replication" }, description = "The replication factor to use for the tablespace, defaults to 1.")
	private Integer replicationFactor = 1;

	@Parameter(required = true, names = { "-np", "--partitions" }, description = "The number of partitions to pre-allocate for the empty tablespace.")
	private Integer nPartitions;

	@Parameter(required = true, names = { "-q", "--qnode" }, description = "A QNode address, will be used for deploying the tablespace.")
	private String qnode;

	@Parameter(required = true, names = { "-tn", "--tablespacename" }, description = "If root is a generated tablespace - for example, a bucket in S3 - this parameter provides its name. Either use this or tablespaces parameter if root contains multiple tablespaces.")
	private String tablespaceName = null;

	@Parameter(names = { "-is", "--init-statements" }, description = "Statements that will be executed just before starting to serve a partition. Ideal place to put PRAGMA statements, like cache-size")
	private List<String> initStatements = new ArrayList<String>();

	private Configuration conf;

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		JCommander jComm = new JCommander(this);
		jComm.setProgramName("Tablespace Creator");
		try {
			jComm.parse(args);
		} catch(ParameterException e) {
			System.out.println(e.getMessage());
			jComm.usage();
			return -1;
		} catch(Throwable t) {
			t.printStackTrace();
			jComm.usage();
			return -1;
		}

		System.out.println("Querying Splout QNode for list of DNodes...");
		SploutClient client = new SploutClient(qnode);
		List<String> dnodes = client.dNodeList();

		ReplicationMap repMap = ReplicationMap.roundRobinMap(nPartitions, replicationFactor, dnodes.toArray(new String[0]));
		DeployInfo info = client.create(tablespaceName, repMap, initStatements);
		
		if(info.getError() == null) {
			System.out.println("Tablespace successfully created: " + info.getStartedAt() + " version: " + info.getVersion());
			return 0;
		} else {
			System.err.println(info.getError());
			return -1;
		}
	}

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new CreatorCMD(), args);
  }
}
