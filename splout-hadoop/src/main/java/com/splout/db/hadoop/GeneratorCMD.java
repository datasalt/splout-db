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
import com.datasalt.pangool.utils.HadoopUtils;
import com.splout.db.common.*;
import com.splout.db.hadoop.TupleSampler.SamplingType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * A general purpose tool for building and deploying any combination of Tablespaces to Splout. It will read JSON
 * configuration files for each tablespace to deploy. Those files must map to the bean {@link JSONTablespaceDefinition}.
 */
public class GeneratorCMD implements Tool {

  private final static Log log = LogFactory.getLog(GeneratorCMD.class);

  @Parameter(required = true, names = {"-tf", "--tablespacefile"}, description = "The JSON config file with the Tablespaces specifications. Multiple files can be provided. Non full qualified URLs forces to load the file from the current Hadoop filesystem.")
  private List<String> tablespaceFiles;

  @Parameter(required = true, names = {"-o", "--output"}, description = "Output path where the generated tablespaces will be saved. If you are running the process from Hadoop, relative paths would use the Hadoop filesystem. Use full qualified URIs instead if you want other behaviour.")
  private String output;

  @Parameter(required = false, names = {"-st", "--sampling-type"}, description = "Selects the sampling type to use. FULL_SCAN: sampling from the full dataset. RANDOM: random selection of samples from the start of splits. ")
  private SamplingType samplingType = SamplingType.FULL_SCAN;

  @Parameter(required = false, names = {"-p", "--parallelism"}, description = "Parallelism to be used. Allows to execute the generation of several tablespaces in parallel.")
  private int parallelism = 1;

  @Parameter(required = false, names = {"-q", "--qnode"}, description = "QNode URL to retrieve partition map to use. To be" +
      " used in the case we want to skip sampling and reuse the existing partition map at Splout. When provided sampling phase is skipped. " +
      "Accepts a CSV list of QNode URLs.")
  private String qnodeURL = null;

  private Configuration conf;

  public int run(String[] args) throws Exception {
    JCommander jComm = new JCommander(this);
    jComm
        .setProgramName("Splout Tablespaces Generator. Generates tablespaces, ready to be deployed to a Splout Cluster.");
    try {
      jComm.parse(args);
    } catch (Throwable t) {
      t.printStackTrace();
      jComm.usage();
      return -1;
    }

    if (parallelism < 1) {
      System.err.println("Parallelism must be greater than 0.");
      System.exit(1);
    }

    log.info("Parsing input parameters...");

    // All the tablespaces that will be generated and deployed atomically, hashed by their name
    // We generate this first so we can detect errors in the configuration before even using Hadoop
    Map<String, TablespaceSpec> tablespacesToGenerate = new HashMap<String, TablespaceSpec>();

    // Partition maps to reuse at indexation. Used when sampling is skipped.
    final Map<String, PartitionMap> partitionMapsToReuse = new HashMap<String, PartitionMap>();

    for (String tablespaceFile : tablespaceFiles) {
      Path file = new Path(tablespaceFile);
      FileSystem fS = FileSystem.get(file.toUri(), getConf());

      if (!fS.exists(file)) {
        throw new IllegalArgumentException("Config input file: " + file + " doesn't exist!");
      }

      String strContents = HadoopUtils.fileToString(fS, file);
      JSONTablespaceDefinition def = JSONSerDe.deSer(strContents, JSONTablespaceDefinition.class);
      TablespaceSpec spec = def.build(conf);
      String name = def.getName();

      tablespacesToGenerate.put(name, spec);

      // Reusing partition maps?
      if (qnodeURL != null) {
        partitionMapsToReuse.put(name, retrievePartitionMapfromQNode(name));
      }
    }

    if (!FileSystem.getLocal(conf).equals(FileSystem.get(conf))) {
      File nativeLibs = new File("native");
      if (nativeLibs.exists()) {
        SploutHadoopConfiguration.addSQLite4JavaNativeLibsToDC(conf);
      }
    }

    Path out = new Path(output);
    FileSystem outFs = out.getFileSystem(getConf());
    HadoopUtils.deleteIfExists(outFs, out);

    ExecutorService executor = Executors.newFixedThreadPool(parallelism);
    ExecutorCompletionService<Boolean> ecs = new ExecutorCompletionService<Boolean>(executor);
    ArrayList<Future<Boolean>> generatorFutures = new ArrayList<Future<Boolean>>();

    // Generate each tablespace
    for (final Map.Entry<String, TablespaceSpec> tablespace : tablespacesToGenerate.entrySet()) {
      Path tablespaceOut = new Path(out, tablespace.getKey());
      TablespaceSpec spec = tablespace.getValue();

      log.info("Generating view with Hadoop (" + tablespace.getKey() + ")");
      final TablespaceGenerator viewGenerator = new TablespaceGenerator(spec, tablespaceOut, this.getClass());

      generatorFutures.add(ecs.submit(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          if (qnodeURL == null) {
            viewGenerator.generateView(conf, samplingType, new TupleSampler.RandomSamplingOptions());
            return true;
          } else {
            viewGenerator.generateView(conf, partitionMapsToReuse.get(tablespace.getKey()));
            return true;
          }
        }
      }));
    }

    // Waiting all tasks to finish.
    for (int i = 0; i < tablespacesToGenerate.size(); i++) {
      // Get will throw an exception if the callable returned it.
      try {
        ecs.take().get();
      } catch (ExecutionException e) {
        // One job was wrong. Stopping the rest.
        for (Future<Boolean> task : generatorFutures) {
          task.cancel(true);
        }
        executor.shutdown();
        throw e;
      }
    }

    executor.shutdown();

    log.info("Done!");
    return 0;
  }

  protected PartitionMap retrievePartitionMapfromQNode(String tablespace) throws Exception {
    log.info("Retrieving partition map for tablespace[" + tablespace + "] from QNode[" + qnodeURL +"] for being reused at indexation. ");
    SploutClient client = new SploutClient(qnodeURL.split(","));
    Tablespace tb = client.tablespace(tablespace);
    if (tb == null) {
      throw new Exception("Partition map for tablespace [" + tablespace + "] not found at QNode[" + qnodeURL +"].");
    }
    PartitionMap pmap = tb.getPartitionMap();
    log.info("Successfully retrieved partition map for tablespace[" + tablespace + "] from QNode[" + qnodeURL +"].");
    return pmap;
  }

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new GeneratorCMD(), args);
  }
}
