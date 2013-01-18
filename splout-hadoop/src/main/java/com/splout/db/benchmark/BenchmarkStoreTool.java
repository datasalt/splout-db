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

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.tuplemr.MapOnlyJobBuilder;
import com.datasalt.pangool.tuplemr.mapred.MapOnlyMapper;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HadoopInputFormat;
import com.datasalt.pangool.utils.HadoopUtils;
import com.splout.db.common.JSONSerDe;
import com.splout.db.common.PartitionEntry;
import com.splout.db.common.PartitionMap;
import com.splout.db.common.SploutHadoopConfiguration;
import com.splout.db.hadoop.NullableTuple;
import com.splout.db.hadoop.TableSpec;
import com.splout.db.hadoop.TupleSQLite4JavaOutputFormat;

/**
 * Distributed map-only job that creates an arbitrarily big database for being used by {@link BenchmarkTool}. It doesn't
 * deploy it.
 */
@SuppressWarnings("serial")
public class BenchmarkStoreTool implements Tool, Serializable {

	@Parameter(required = true, names = { "-o", "--output" }, description = "Output path where store will be generated. It must be accessible for all Splout nodes for the deploy to succeed.")
	private String output;

	@Parameter(names = { "-t", "--tablename" }, description = "Name of the table that will be used for inserting data.")
	private String tablename = "splout_benchmark";

	@Parameter(required = true, names = { "-k", "--keyspace" }, description = "A representation of a key space used for generating the data store. Format is minKey:maxKey where both are considered integers. A spec of 0:10 means keys range from 0 (minimum value) to 10 (maximum value).")
	private String keySpace;

	@Parameter(required = true, names = { "-p", "--partitions" }, description = "The number of partitions to create for the tablespace.")
	private Integer nPartitions;

	@Parameter(names = { "-s", "--valuesize" }, description = "The value size in bytes that will be associated with each key.")
	private Integer valueSize = 1024;
	
	@Parameter(names = { "-pad", "--padding" }, description = "The padding size to use for normalizing the integer keys to strings. With padding 3, 1 gets 001. This is needed for benchmark keys. By default, padding is autoadjusted to the size of the maxKey of the key range.")
	private	Long padding;

	private transient Configuration conf;

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
		jComm.setProgramName("Benchmark-Store Tool");
		try {
			jComm.parse(args);
		} catch (ParameterException e){
			System.out.println(e.getMessage());
			jComm.usage();
			return -1;
		} catch(Throwable t) {
			t.printStackTrace();
			jComm.usage();
			return -1;
		}

		// Create some input files that will represent the partitions to generate
		Path out = new Path(output);
		FileSystem outFs = out.getFileSystem(getConf());
		HadoopUtils.deleteIfExists(outFs, out);

		Integer min, max, eachPartition;
		int maxKeyDigits;
		
		try {
			String[] minMax = keySpace.split(":");
			min = Integer.parseInt(minMax[0]);
			max = Integer.parseInt(minMax[1]);
			maxKeyDigits = max.toString().length();
			
			eachPartition = (max - min) / nPartitions;
		} catch(Exception e) {
			throw new IllegalArgumentException(
			    "Key range format is not valid. It must be minKey:maxKey where both minKey and maxKey are integers.");
		}

		FileSystem inFs = FileSystem.get(getConf());
		Path input = new Path("benchmark-store-tool-" + System.currentTimeMillis());
		HadoopUtils.deleteIfExists(inFs, input);
		inFs.mkdirs(input);

		List<PartitionEntry> partitionEntries = new ArrayList<PartitionEntry>();

		// Create as many input files as partitions
		// Each input file will have as value the range that each Mapper will write
		String paddingExp = "%0" + (padding != null ? padding : maxKeyDigits) + "d";
		for(int i = 0; i < nPartitions; i++) {
			int thisMin = (i * eachPartition);
			int thisMax = (i + 1) * eachPartition;
			HadoopUtils.stringToFile(inFs, new Path(input, i + ".txt"), i + "\t" + thisMin + ":" + thisMax);
			PartitionEntry entry = new PartitionEntry();
			entry.setMin(String.format(paddingExp, thisMin));
			entry.setMax(String.format(paddingExp, thisMax));
			entry.setShard(i);
			partitionEntries.add(entry);
		}

		partitionEntries.get(0).setMin(null);
		partitionEntries.get(partitionEntries.size() - 1).setMax(null);
		
		PartitionMap partitionMap = new PartitionMap(partitionEntries);
		HadoopUtils.stringToFile(outFs, new Path(out, "partition-map"), JSONSerDe.ser(partitionMap));

		List<Field> fields = new ArrayList<Field>();
		fields.add(Field.create(TupleSQLite4JavaOutputFormat.PARTITION_TUPLE_FIELD, Type.INT));
		fields.addAll(Fields.parse("key:int, value:string"));
		final Schema schema = new Schema(tablename, fields);

		byte[] valueArray = new byte[valueSize];
		for(int i = 0; i < valueSize; i++) {
			valueArray[i] = 'A';
		}
		final String theValue = new String(valueArray);

		if(!FileSystem.getLocal(conf).equals(FileSystem.get(conf))) {
			File nativeLibs = new File("native");
			if(nativeLibs.exists()) {
				SploutHadoopConfiguration.addSQLite4JavaNativeLibsToDC(conf);
			}
		}
		
		MapOnlyJobBuilder job = new MapOnlyJobBuilder(conf);
		TableSpec tableSpec = new TableSpec(schema, schema.getFields().get(1));
		
		job.setOutput(new Path(out, "store"), new TupleSQLite4JavaOutputFormat(1000000, tableSpec), ITuple.class,
		    NullWritable.class);
		job.addInput(input, new HadoopInputFormat(TextInputFormat.class), new MapOnlyMapper<LongWritable, Text, ITuple, NullWritable>() {

			ITuple metaTuple = new NullableTuple(schema);

			protected void map(LongWritable key, Text value, Context context) throws IOException,
			    InterruptedException {

				String[] partitionRange = value.toString().split("\t");
				Integer partition = Integer.parseInt(partitionRange[0]);
				metaTuple.set(TupleSQLite4JavaOutputFormat.PARTITION_TUPLE_FIELD, partition);
				String[] minMax = partitionRange[1].split(":");
				Integer min = Integer.parseInt(minMax[0]);
				Integer max = Integer.parseInt(minMax[1]);
				for(int i = min; i < max; i++) {
					metaTuple.set("key", i);
					metaTuple.set("value", theValue);
					context.write(metaTuple, NullWritable.get());
				}
			}
		});

		job.createJob().waitForCompletion(true);

		HadoopUtils.deleteIfExists(inFs, input);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new BenchmarkStoreTool(), args);
	}
}
