package com.splout.db.integration;

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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.splout.db.common.SploutClient;
import com.splout.db.common.SploutHadoopConfiguration;
import com.splout.db.engine.DefaultEngine;
import com.splout.db.hadoop.DeployerCMD;
import com.splout.db.hadoop.SimpleGeneratorCMD;
import com.splout.db.qnode.beans.QNodeStatus;
import com.splout.db.qnode.beans.QueryStatus;

/**
 * This program runs an integration test to assert that different versions of Hadoop work well with Splout.
 * <p>
 * Versions tested successfully so far: Apache 0.20.X, CDH3 and Apache 1.0.4
 */
public class HadoopIntegrationTest implements Tool, Configurable {

	@Parameter(names = { "-e", "--engine" }, description = "Optionally, use a specific engine for integration-testing it (otherwise default is used).")
	private String engine = DefaultEngine.class.getName();

	@Parameter(names = { "-q", "--qnode" }, description = "The QNode address.")
	private String qnode = "http://localhost:4412";

	@Parameter(names = { "-i", "--input" }, description = "The page counts sample file to use as input, otherwise the one in src/main/resources is used. Override if needed.")
	private String input = "src/main/resources/pagecounts-sample/pagecounts-20090430-230000-sample";

	Configuration conf;

	@Override
	public int run(String[] args) throws Exception {
		// Validate params etc
		JCommander jComm = new JCommander(this);
		jComm.setProgramName("Splout Hadoop Compatibility Integration Test");
		try {
			jComm.parse(args);
		} catch(ParameterException e) {
			System.err.println(e.getMessage());
			jComm.usage();
			System.exit(-1);
		}
		
		Path tmpHdfsPath = new Path("tmp-" + HadoopIntegrationTest.class.getName() + "-"
		    + System.currentTimeMillis());
		FileSystem fS = tmpHdfsPath.getFileSystem(getConf());
		fS.mkdirs(tmpHdfsPath);
		fS.mkdirs(new Path(tmpHdfsPath, "input"));
		fS.mkdirs(new Path(tmpHdfsPath, "output"));
		boolean isLocal = FileSystem.get(conf).equals(FileSystem.getLocal(conf));
		if(!isLocal) {
			SploutHadoopConfiguration.addSQLite4JavaNativeLibsToDC(conf);
		}

		tmpHdfsPath = tmpHdfsPath.makeQualified(fS);

		Path pageCounts = new Path(input);
		FileUtil.copy(FileSystem.getLocal(getConf()), pageCounts, fS, new Path(tmpHdfsPath, "input"), false,
		    getConf());
		
		SimpleGeneratorCMD generator = new SimpleGeneratorCMD();
		generator.setConf(getConf());
		if(generator.run(new String[] { "-tb", "pagecountsintegration", "-t", "pagecounts", "-i",
		    tmpHdfsPath + "/input", "-o", tmpHdfsPath + "/output", "-s",
		    "projectcode:string, pagename:string, visits:int, bytes:long", "-pby", "projectcode,pagename",
		    "-sep", "\" \"", "-p", "2", "-e", engine }) < 0) {
			throw new RuntimeException("Generator failed!");
		}

		SploutClient client = new SploutClient(qnode);
		QNodeStatus status = client.overview();
		long previousVersion = -1;
		if(status.getTablespaceMap().get("pagecountsintegration") != null) {
			previousVersion = status.getTablespaceMap().get("pagecountsintegration").getVersion();
		}

		DeployerCMD deployer = new DeployerCMD();
		deployer.setConf(getConf());
		if(deployer.run(new String[] { "-r", "2", "-q", qnode, "-root", tmpHdfsPath + "/output", "-ts",
		    "pagecountsintegration" }) < 0) {
			throw new RuntimeException("Deployer failed!");
		}

		long waitedSoFar = 0;

		status = client.overview();
		while(status.getTablespaceMap().get("pagecountsintegration") == null
		    || previousVersion == status.getTablespaceMap().get("pagecountsintegration").getVersion()) {
			Thread.sleep(2000);
			waitedSoFar += 2000;
			status = client.overview();
			if(waitedSoFar > 90000) {
				throw new RuntimeException(
				    "Deploy must have failed in Splout's server. Waiting too much for it to complete.");
			}
		}

		previousVersion = status.getTablespaceMap().get("pagecountsintegration").getVersion();

		QueryStatus qStatus = client.query("pagecountsintegration", "*", "SELECT * FROM pagecounts;", null);
		System.out.println(qStatus.getResult());

		if(qStatus.getResult() == null) {
			throw new RuntimeException("Something failed as query() is returning null!");
		}

		System.out.println("Everything fine.");
		return 1;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new HadoopIntegrationTest(), args);
	}
}
