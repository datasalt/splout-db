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

import com.datasalt.pangool.io.*;
import com.datasalt.pangool.tuplemr.mapred.lib.input.TupleInputFormat;
import com.splout.db.hadoop.TupleSampler.SamplingType;
import com.splout.db.hadoop.TupleSampler.TupleSamplerException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestTupleSampler {

  public static String INPUT = "input-" + TestTupleSampler.class.getName();
  public static String OUTPUT = "output-" + TestTupleSampler.class.getName();

  @Test
  public void testDefault() throws IOException, InterruptedException, TupleSamplerException {
    testDefault(Long.MAX_VALUE, 1);
    testDefault(1024 * 100, 2);
    testDefault(1024 * 10, 3);
  }

  @Test
  public void testReservoir() throws IOException, InterruptedException, TupleSamplerException {
    Runtime.getRuntime().exec("rm -rf " + INPUT + "_r");
    Runtime.getRuntime().exec("rm -rf " + OUTPUT + "_r");

    Configuration conf = new Configuration();

    TupleFile.Writer writer = new TupleFile.Writer(FileSystem.get(conf), conf, new Path(INPUT + "_r"), schema);
    for (int i = 0; i < 10000; i++) {
      ITuple tuple = new Tuple(schema);
      tuple.set("id", i + "");
      // We save a number in the "foo" field which is consecutive: [0, 1, 2, ... 9999]
      tuple.set("foo", "foo" + i);
      writer.append(tuple);
    }
    writer.close();

    // Sampling with default method should yield lower numbers
    // Default input split size so only one InputSplit
    // All sampled keys will be [0, 1, 2, ..., 9]
    TupleSampler sampler = new TupleSampler(SamplingType.RANDOM, new TupleSampler.RandomSamplingOptions(), this.getClass());
    //sampler.sample(Arrays.asList(new TableInput[] { new TableInput(new TupleInputFormat(), new HashMap<String, String>(), schema, new IdentityRecordProcessor(), new Path(INPUT + "_r")) }), schema, conf, 10, new Path(OUTPUT + "_r"));
    sampler.sample(getTblSpec("r"), conf, 10, new Path(OUTPUT + "_r"));

    int nTuples = 0;
    int[] sampledKeys = new int[10];

    SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), new Path(OUTPUT + "_r"), conf);
    Text key = new Text();
    while (reader.next(key)) {
      sampledKeys[nTuples] = Integer.parseInt(key.toString());
      nTuples++;
    }
    reader.close();

    for (int i = 0; i < 10; i++) {
      assertEquals(i, sampledKeys[i]);
    }

    // Reservoir sampling should yield better results for this case, let's see
    sampler = new TupleSampler(SamplingType.FULL_SCAN, new TupleSampler.RandomSamplingOptions(), this.getClass());
    // sampler.sample(Arrays.asList(new TableInput[] { new TableInput(new TupleInputFormat(), new HashMap<String, String>(), schema, new IdentityRecordProcessor(), new Path(INPUT + "_r")) }), schema, conf, 10, new Path(OUTPUT + "_r"));
    sampler.sample(getTblSpec("r"), conf, 10, new Path(OUTPUT + "_r"));

    nTuples = 0;
    sampledKeys = new int[10];

    reader = new SequenceFile.Reader(FileSystem.get(conf), new Path(OUTPUT + "_r"), conf);
    key = new Text();
    while (reader.next(key)) {
      sampledKeys[nTuples] = Integer.parseInt(key.toString());
      nTuples++;
    }
    reader.close();

    int avgKey = 0;
    for (int i = 0; i < 10; i++) {
      avgKey += sampledKeys[i];
    }

    avgKey = avgKey / 10;
    // This assertion may fail some day... but the chances are very rare.
    // The lower bound is very low, usually the average key will be around 1/4th of the max key (10000).
    assertTrue(avgKey > 100);

    Runtime.getRuntime().exec("rm -rf " + INPUT + "_r");
    Runtime.getRuntime().exec("rm -rf " + OUTPUT + "_r");
  }

  public void testDefault(long splitSize, int iter) throws TupleSamplerException, IOException, InterruptedException {
    Runtime.getRuntime().exec("rm -rf " + INPUT + "_" + iter);
    Runtime.getRuntime().exec("rm -rf " + OUTPUT + "_" + iter);

    Configuration conf = new Configuration();

    TupleFile.Writer writer = new TupleFile.Writer(FileSystem.get(conf), conf, new Path(INPUT + "_" + iter), schema);
    for (int i = 0; i < 1000; i++) {
      writer.append(randomTuple());
    }
    writer.close();

    // Requesting as many samples as splits so one sample is needed from each split.
    FileStatus status = FileSystem.get(conf).getFileStatus(new Path(INPUT + "_" + iter));
    long expectedSplits = Math.max(1, (long) Math.ceil(((double) status.getLen()) / splitSize));

    TupleSampler.RandomSamplingOptions options = new TupleSampler.RandomSamplingOptions();
    options.setMaxInputSplitSize(splitSize);
    options.setMaxSplitsToVisit(Integer.MAX_VALUE);

    TablespaceSpec tablespaceSpec = getTblSpec(iter + "");

    TupleSampler sampler = new TupleSampler(SamplingType.RANDOM, options, this.getClass());
    sampler.sample(tablespaceSpec, conf, expectedSplits, new Path(OUTPUT + "_" + iter));

    int nKeys = 0;
    SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), new Path(OUTPUT + "_" + iter), conf);
    Text key = new Text();
    while (reader.next(key)) {
      nKeys++;
    }
    reader.close();

    String a = "";
    a.split("foo");

    assertEquals(expectedSplits, nKeys);

    Runtime.getRuntime().exec("rm -rf " + INPUT + "_" + iter);
    Runtime.getRuntime().exec("rm -rf " + OUTPUT + "_" + iter);
  }

  final Schema schema = new Schema("schema", Fields.parse("id:string, foo:string"));

  public ITuple randomTuple() {
    ITuple tuple = new Tuple(schema);
    tuple.set("id", "id" + (Math.random() * 1000000000));
    tuple.set("foo", "foobar" + (Math.random() * 1000000000));
    return tuple;
  }

  public TablespaceSpec getTblSpec(String inputPostfix) {
    return new TablespaceSpec(
        Arrays.asList(
            new Table(
                Arrays.asList(
                    new TableInput[]{
                        new TableInput(
                            new TupleInputFormat(),
                            new HashMap<String, String>(),
                            schema,
                            new IdentityRecordProcessor(),
                            new Path(INPUT + "_" + inputPostfix))}),
                new TableSpec(
                    schema,
                    schema.getField("id"))

            )),
        null, 1, null, null);
  }
}
