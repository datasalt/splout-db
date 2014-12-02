package com.splout.db.hadoop.engine;

/*
 * #%L
 * Splout SQL Hadoop library
 * %%
 * Copyright (C) 2012 - 2014 Datasalt Systems S.L.
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

import com.datasalt.pangool.io.ITuple;
import com.splout.db.common.HeartBeater;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The actual outputformat that is used in Splout SQL database generation. It receives a {@link SploutSQLOutputFormat}
 * by constructor. This outputformat performs the common tasks: heart beating, asking for a temporary folder to write data
 * with the Hadoop API, completing the output, etc.
 */
@SuppressWarnings("serial")
public class SploutSQLProxyOutputFormat extends FileOutputFormat<ITuple, NullWritable> implements Serializable {

  private SploutSQLOutputFormat outputFormat;

  public SploutSQLProxyOutputFormat(SploutSQLOutputFormat outputFormat) {
    this.outputFormat = outputFormat;
  }

  private static AtomicLong FILE_SEQUENCE = new AtomicLong(0);
  private HeartBeater heartBeater;
  private Configuration conf;
  TaskAttemptContext context;

  @Override
  public RecordWriter<ITuple, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException,
      InterruptedException {

    long waitTimeHeartBeater = context.getConfiguration().getLong(HeartBeater.WAIT_TIME_CONF, 5000);
    heartBeater = new HeartBeater(context, waitTimeHeartBeater);
    heartBeater.needHeartBeat();
    conf = context.getConfiguration();
    this.context = context;

    outputFormat.setConf(context.getConfiguration());

    return new RecordWriter<ITuple, NullWritable>() {

      // Temporary and permanent Paths for properly writing Hadoop output files
      private Map<Integer, Path> permPool = new HashMap<Integer, Path>();
      private Map<Integer, Path> tempPool = new HashMap<Integer, Path>();

      private void initSql(int partition) throws IOException, InterruptedException {
        // HDFS final location of the generated partition file. It will be
        // loaded to the temporary folder in the HDFS than finally will be
        // committed by the OutputCommitter to the proper location.
        FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(SploutSQLProxyOutputFormat.this.context);
        Path perm = new Path(committer.getWorkPath(), partition + ".db");
        FileSystem fs = perm.getFileSystem(conf);

        // Make a task unique name that contains the actual index output name to
        // make debugging simpler
        // Note: if using JVM reuse, the sequence number will not be reset for a
        // new task using the jvm
        Path temp = conf.getLocalPath("mapred.local.dir", "splout_task_" + SploutSQLProxyOutputFormat.this.context.getTaskAttemptID()
            + '.' + FILE_SEQUENCE.incrementAndGet());

        FileSystem localFileSystem = FileSystem.getLocal(conf);
        if (localFileSystem.exists(temp)) {
          localFileSystem.delete(temp, true);
        }
        localFileSystem.mkdirs(temp);

        Path local = fs.startLocalOutput(perm, new Path(temp, partition + ".db"));

        //
        permPool.put(partition, perm);
        tempPool.put(partition, new Path(temp, partition + ".db"));

        outputFormat.initPartition(partition, local);
      }

      @Override
      public void close(TaskAttemptContext ctx) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(ctx.getConfiguration());
        try {
          if (ctx != null) {
            heartBeater.setProgress(ctx);
          }
          outputFormat.close();
          for (Map.Entry<Integer, Path> entry : permPool.entrySet()) {
            // Hadoop - completeLocalOutput()
            fs.completeLocalOutput(entry.getValue(), tempPool.get(entry.getKey()));
          }
        } finally { // in any case, destroy the HeartBeater
          heartBeater.cancelHeartBeat();
        }
      }

      @Override
      public void write(ITuple tuple, NullWritable ignore) throws IOException, InterruptedException {
        int partition = (Integer) tuple.get(SploutSQLOutputFormat.PARTITION_TUPLE_FIELD);
        if (tempPool.get(partition) == null) {
          initSql(partition);
        }
        outputFormat.write(tuple);
      }

    };
  }

}
