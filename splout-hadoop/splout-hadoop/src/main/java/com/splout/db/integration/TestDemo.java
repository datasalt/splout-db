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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.mortbay.log.Log;

import com.datasalt.pangool.tuplemr.mapred.lib.input.TupleInputFormat;
import com.datasalt.pangool.tuplemr.mapred.lib.output.TupleOutputFormat.TupleRecordWriter;
import com.datasalt.pangool.utils.HadoopUtils;
import com.splout.db.common.PartitionMap;
import com.splout.db.common.ReplicationMap;
import com.splout.db.common.SploutClient;
import com.splout.db.common.SploutHadoopTestUtils;
import com.splout.db.hadoop.TablespaceSpec;
import com.splout.db.hadoop.TablespaceGenerator;
import com.splout.db.hadoop.TupleSampler;
import com.splout.db.hadoop.TupleSampler.SamplingType;

/**
 * Generates random test Tuples (one string, one int) and deploys them to Splout. This is the "minimum-viable" demo for
 * Splout. Use the main() method for running it.
 */
public class TestDemo {

	public void generate(int nPartitions, long nRegs, String dnodes, String qnode, Path inputPath, Path outputPath)
	    throws Exception {
		Configuration conf = new Configuration();

		FileSystem fS = FileSystem.get(conf);
		HadoopUtils.deleteIfExists(fS, inputPath);
		HadoopUtils.deleteIfExists(fS, outputPath);

		NullWritable nullValue = NullWritable.get();
		TupleRecordWriter writer = TupleRecordWriter.getTupleWriter(conf, SploutHadoopTestUtils.SCHEMA, fS.create(inputPath));
		
		// Writes nRegs Tuples to HDFS
		long soFar = 0;
		while(soFar < nRegs) {
			writer.write(SploutHadoopTestUtils.getTuple("id" + soFar, (int) soFar), nullValue);
			soFar++;
		}
		writer.close(null);
		
		// Generate Splout view
		TablespaceSpec tablespace = TablespaceSpec.of(SploutHadoopTestUtils.SCHEMA, "id", inputPath, new TupleInputFormat(), nPartitions);
		TablespaceGenerator generateView = new TablespaceGenerator(tablespace, outputPath);
		generateView.generateView(conf, SamplingType.DEFAULT, new TupleSampler.DefaultSamplingOptions());
		PartitionMap partitionMap = generateView.getPartitionMap();
		ReplicationMap replicationMap = ReplicationMap.oneToOneMap(dnodes.split(","));

		Path deployUri = new Path(outputPath, "store").makeQualified(fS);

		SploutClient client = new SploutClient(qnode);
		client.deploy("tablespace1", partitionMap, replicationMap, deployUri.toUri());
	}

	public static void main(String[] args) throws Exception {
		if(args.length != 4) {
			System.err.println("Wrong arguments provided.\n\n");
			System.out
			    .println("Usage: [nPartitions] [dNodes] [qNode] [nRegisters] \n\nExample: 2 localhost:9002,localhost:9003 localhost:9001 100000 \n");
			System.exit(-1);
		}
		int nPartitions = Integer.parseInt(args[0]);
		String dnodes = args[1];
		String qnode = args[2];
		long nRegisters = Long.parseLong(args[3]);
		TestDemo generator = new TestDemo();
		Log.info("Parameters: partitions=" + nPartitions + ", registers=" + nRegisters + " dnodes: [" + dnodes + "]");

		generator.generate(nPartitions, nRegisters, dnodes, qnode, new Path("in-generate"), new Path("out-generate"));
	}
}
