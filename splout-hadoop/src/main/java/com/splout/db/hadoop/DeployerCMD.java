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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.io.Files;
import com.splout.db.common.JSONSerDe;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.type.TypeReference;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * This tool can be used to deploy tablespaces once they are generated.
 * It allows to deploy several tablespaces at a time.
 * You can specify here the replication factor and the QNode to deploy to.
 */
public class DeployerCMD implements Tool {

  @Parameter(names = {"-r", "--replication"}, description = "The replication factor to use for all the tablespace, defaults to 1.")
  private Integer replicationFactor = 1;

  @Parameter(required = true, names = {"-q", "--qnode"}, description = "A QNode address, will be used for deploying the tablespace.")
  private String qnode;

  @Parameter(names = {"-root", "--root"}, description = "The path where the generated tablespace or tablespaces are. If you are running the process from Hadoop, relative paths would use the Hadoop filesystem. Use full qualified URIs instead if you want other behaviour.")
  private String root;

  @Parameter(names = {"-tn", "--tablespacename"}, description = "If root is a generated tablespace - for example, a bucket in S3 - this parameter provides its name. Either use this or tablespaces parameter if root contains multiple tablespaces.")
  private String tablespaceName = null;

  @Parameter(names = {"-ts", "--tablespaces"}, description = "If root contains more than one tablespace, specify here name of the tablespaces that should be deployed. A folder with the partition-map file and the store folder should be into the path <root>/<tablespace>. Several can be provided.")
  private List<String> tablespaces = new ArrayList<String>();

  @Parameter(names = {"-c", "--config-file"}, description = "Use the given filename as deployment spec. If present, <root> <tablespace> <repfactor> and <init-statements> options are ignored. In local filesystem.")
  private String configFile = null;

  @Parameter(names = {"-is", "--init-statements"}, description = "Statements that will be executed just before starting to serve a partition. Ideal place to put PRAGMA statements, like cache-size")
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
    jComm.setProgramName("Tablespaces Deployer");
    try {
      jComm.parse(args);
    } catch (ParameterException e) {
      System.out.println(e.getMessage());
      jComm.usage();
      return -1;
    } catch (Throwable t) {
      t.printStackTrace();
      jComm.usage();
      return -1;
    }

    StoreDeployerTool deployer = new StoreDeployerTool(qnode, getConf());

    ArrayList<TablespaceDepSpec> deployments = new ArrayList<TablespaceDepSpec>();
    if (configFile != null) {
      deployments = JSONSerDe.deSer(Files.toString(new File(configFile), Charset.forName("UTF-8")), new TypeReference<ArrayList<TablespaceDepSpec>>() {
      });
    } else {
      Path rootPath = new Path(root);
      if (tablespaceName == null && tablespaces.size() == 0) {
        System.err.println("Tablespace name for root folder or tablespaces contained in them is lacking. Either use tablespacename or tablespaces option.");
        jComm.usage();
        return -1;
      }
      if (tablespaceName != null && tablespaces.size() > 0) {
        System.err.println("Can't use tablespacename and tablespaces at the same time. Root is to be either a generated tablespace or a folder with multiple generated tablespaces.");
        jComm.usage();
        return -1;
      }
      if (tablespaceName != null) {
        deployments.add(new TablespaceDepSpec(tablespaceName, rootPath.toString(), replicationFactor, initStatements));
      }
      for (String tb : tablespaces) {
        Path tablespacePath = new Path(rootPath, tb);
        deployments.add(new TablespaceDepSpec(tb, tablespacePath.toString(), replicationFactor, initStatements));
      }
    }

    // Checking for file existence
    for (TablespaceDepSpec spec : deployments) {
      Path tablespacePath = new Path(spec.getSourcePath());
      FileSystem fs = tablespacePath.getFileSystem(getConf());
      if (!fs.exists(tablespacePath)) {
        System.err.println("ERROR: Path [" + tablespacePath.makeQualified(fs) + "] not found.");
        return 1;
      }
    }

    deployer.deploy(deployments);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new DeployerCMD(), args);
  }
}
