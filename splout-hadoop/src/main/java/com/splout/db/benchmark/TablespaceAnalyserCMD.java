package com.splout.db.benchmark;

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
import com.datasalt.pangool.utils.HadoopUtils;
import com.google.common.base.Joiner;
import com.google.common.collect.HashBasedTable;
import com.google.common.io.Files;
import com.splout.db.common.JSONSerDe;
import com.splout.db.common.SploutClient;
import com.splout.db.common.Tablespace;
import com.splout.db.hadoop.JSONTablespaceDefinition;
import com.splout.db.qnode.beans.QNodeStatus;
import com.splout.db.qnode.beans.QueryStatus;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.Map;

public class TablespaceAnalyserCMD extends Configured implements Tool {

  @Parameter(required = true, names = {"-tf", "--tablespacefile"}, description = "The JSON config file with the Tablespace specifications. Multiple files can be provided. Non full qualified URLs forces to load the file from the current Hadoop filesystem.")
  private String tablespaceFile;

  @Parameter(required = true, names = {"-q", "--qnodes"}, description = "Comma-separated list QNode addresses.")
  private String qNodes;

  @Parameter(required = false, names = {"-t", "--top-size"}, description = "Size of calculated tops")
  private int topSize = 10;

  protected JSONTablespaceDefinition loadTablespaceFile(String tablespaceFile) throws IOException, JSONSerDe.JSONSerDeException {
    Path file = new Path(tablespaceFile);
    FileSystem fS = FileSystem.get(file.toUri(), getConf());

    if (!fS.exists(file)) {
      throw new IllegalArgumentException("Config input file: " + file + " doesn't exist!");
    }

    String strContents = HadoopUtils.fileToString(fS, file);
    JSONTablespaceDefinition def = JSONSerDe.deSer(strContents, JSONTablespaceDefinition.class);
    return def;
  }

  /*protected String totalRowsSQL(TablespaceSpec spec) {
    String query = "SELECT SUM(*) FROM (";
    for(int i =0 ; i<spec.getPartitionedTables().size(); i++){
      Table table = spec.getPartitionedTables().get(i);
      String tblName = table.getTableSpec().getSchema().getName();
      query += "SELECT COUNT(*) FROM " +  tblName + " ";
    }
  } */

  public int start() throws Exception {
    JSONTablespaceDefinition def = loadTablespaceFile(tablespaceFile);
    String tsName = def.getName();

    SploutClient client = new SploutClient(1000 * 60 * 60 * 24, qNodes.split(","));
    QNodeStatus overview = client.overview();

    if (overview.getTablespaceMap().get(tsName) == null) {
      System.out.println("Tablespace " + tsName + " not found in QNodes " + qNodes + ".");
    }

    Tablespace tablespace = overview.getTablespaceMap().get(tsName);
    int nPartitions = tablespace.getPartitionMap().getPartitionEntries().size();

    System.out.println("TABLESPACE [" + tsName + "]");
    System.out.println("#Partitions: " + nPartitions);

    HashBasedTable<Integer, String, Long> counts = HashBasedTable.create();
    HashBasedTable<Integer, String, LinkedHashMap<String, Long>> tops = HashBasedTable.create();
    for (int part = 0; part < nPartitions; part++) {
      for (int i = 0; i < def.getPartitionedTables().size(); i++) {
        JSONTablespaceDefinition.JSONTableDefinition table = def.getPartitionedTables().get(i);

        String tblName = table.getName();
        String query = "SELECT COUNT(*) FROM " + tblName;

        QueryStatus status = client.query(tsName, null, query, part + "");
        if (status.getError() != null) {
          throw new Exception("Query error: " + status.getError());
        }
        System.out.println(query + ": " + JSONSerDe.ser(status));
        long count = (Integer) ((Map) status.getResult().get(0)).values().iterator().next();
        counts.put(part, tblName, count);

        String partFields[] = table.getPartitionFields().split(",");
        String concatFields = Joiner.on("||").join(partFields);
        query = "SELECT " + concatFields + " key, COUNT(*) c FROM "
            + tblName + " GROUP BY key ORDER by c DESC LIMIT " + topSize;

        status = client.query(tsName, null, query, part + "");
        if (status.getError() != null) {
          throw new Exception("Query error: " + status.getError());
        }
        System.out.println(query + ": " + JSONSerDe.ser(status));
        LinkedHashMap<String, Long> top = new LinkedHashMap<String, Long>();
        for (Map row : (ArrayList<Map<String, Long>>) status.getResult()) {
          top.put(row.get("key").toString(), new Long(row.get("c").toString()));
        }
        tops.put(part, tblName, top);
      }
    }

    Hashtable<String, Long> totalsPerTable = new Hashtable<String, Long>();
    for (String table : counts.columnKeySet()) {
      long count = 0;
      for (Map.Entry<Integer, Long> entry : counts.column(table).entrySet()) {
        count += entry.getValue();
      }
      totalsPerTable.put(table, count);
    }

    BufferedWriter countsFile = Files.newWriter(new File(tsName + "-counts.txt"), Charset.defaultCharset());
    countsFile.write("Table\tPartition\tRows\tPercent from total rows\n");
    for (String table : counts.columnKeySet()) {
      for (int partition : counts.column(table).keySet()) {
        long count = counts.get(partition, table);
        long total = totalsPerTable.get(table);
        double percent = count / (double) total;
        countsFile.write(table + "\t" + partition + "\t" + count + "\t" + percent + "\n");
      }
    }
    countsFile.close();

    BufferedWriter topsFile = Files.newWriter(new File(tsName + "-tops.txt"), Charset.defaultCharset());
    topsFile.write("Table\tPartition\tKey\tRows\tPercent from total rows\n");
    for (String table : tops.columnKeySet()) {
      for (int partition : tops.column(table).keySet()) {
        long total = totalsPerTable.get(table);
        LinkedHashMap<String, Long> top = tops.get(partition, table);

        for (Map.Entry<String, Long> entry : top.entrySet()) {
          double percent = entry.getValue() / (double) total;
          topsFile.write(table + "\t" + partition + "\t" + entry.getKey() + "\t" + entry.getValue() + "\t" + percent + "\n");
        }
      }
    }
    topsFile.close();

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new TablespaceAnalyserCMD(), args);
  }

  @Override
  public int run(String[] args) throws Exception {
    JCommander jComm = new JCommander(this);
    jComm.setProgramName("Tablespace Analyser Tool");
    try {
      jComm.parse(args);
    } catch (ParameterException e) {
      System.out.println(e.getMessage());
      System.out.println();
      jComm.usage();
      return -1;
    } catch (Throwable t) {
      t.printStackTrace();
      jComm.usage();
      return -1;
    }

    return start();
  }
}