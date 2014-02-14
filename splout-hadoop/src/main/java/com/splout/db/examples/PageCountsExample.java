package com.splout.db.examples;

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
import java.io.Serializable;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mortbay.log.Log;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.tuplemr.OrderBy;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.mapred.lib.input.TupleTextInputFormat;
import com.datasalt.pangool.tuplemr.mapred.lib.output.TupleOutputFormat;
import com.datasalt.pangool.utils.HadoopUtils;
import com.splout.db.common.PartitionMap;
import com.splout.db.common.SploutHadoopConfiguration;
import com.splout.db.hadoop.StoreDeployerTool;
import com.splout.db.hadoop.TableBuilder;
import com.splout.db.hadoop.TablespaceBuilder;
import com.splout.db.hadoop.TablespaceDepSpec;
import com.splout.db.hadoop.TablespaceGenerator;
import com.splout.db.hadoop.TupleSampler;
import com.splout.db.hadoop.TupleSampler.SamplingOptions;
import com.splout.db.hadoop.TupleSampler.SamplingType;

/**
 * An advanced Splout example with the Wikipedia pagecounts dataset: http://dom.as/2007/12/10/wikipedia-page-counters/
 */
@SuppressWarnings("serial")
public class PageCountsExample implements Tool, Serializable {

	@Parameter(required = true, names = { "-i", "--inputpath" }, description = "The input path that contains the pagecounts file tree.")
	private String inputPath;

	@Parameter(required = true, names = { "-np", "--npartitions" }, description = "The number of partitions to create.")
	private Integer nPartitions;

	@Parameter(required = true, names = { "-o", "--outputpath" }, description = "The output path.")
	private String outputPath;

	@Parameter(names = { "-d", "--deploy" }, description = "Will deploy the generated dataset.")
	private boolean deploy = false;

	@Parameter(names = { "-q", "--qnode" }, description = "If -d is used, this qnode will be used for deploying the dataset.")
	private String qnode = null;

	@Parameter(names = { "-ng", "--nogenerate" }, description = "If used, the dataset will not be generated, instead, it is expected to be found in the output (-o) path. Use this in conjunction with -d for deploying a previously generated dataset.")
	private boolean noGenerate = false;

	@Parameter(names = { "-r", "--repfactor" }, description = "The replication factor to use when deploying.")
	private Integer repFactor = 1;

	@Parameter(names = { "-m", "--memoryForIndexing" }, description = "The amount of memory to use in each Reducer for indexing, in bytes.")
	private Long memoryForIndexing = 268435456l; // 256 MB

	@Parameter(names = { "-gtf", "--generateTupleFiles" }, description = "This boolean parameter is made for benchmarking purposes. When enabled, no SQLite files will be generated, just plain binary TupleFiles. This allows us to compare the performance of the SQLite output format compared to a plain binary output format. This option is incompatible with -d (deploy).")
	private boolean generateTupleFiles = false;

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
		// Validate params etc
		JCommander jComm = new JCommander(this);
		jComm.setProgramName("Splout Page Counts example");
		try {
			jComm.parse(args);
		} catch(ParameterException e) {
			System.err.println(e.getMessage());
			jComm.usage();
			System.exit(-1);
		}

		boolean generate = !noGenerate; // just for clarifying

		if(generateTupleFiles && deploy) {
			System.err.println("Can't run a 'dry' TupleFile generation and deploy it.");
			jComm.usage();
			System.exit(-1);
		}
		
		Path outPath = new Path(outputPath);
		FileSystem outFs = outPath.getFileSystem(getConf());

		if(!FileSystem.getLocal(conf).equals(FileSystem.get(conf))) {
			File nativeLibs = new File("native");
			if(nativeLibs.exists()) {
				SploutHadoopConfiguration.addSQLite4JavaNativeLibsToDC(conf);
			}
		}

		if(generate) {
			Path inputPath = new Path(this.inputPath);
			FileSystem inputFileSystem = inputPath.getFileSystem(conf);

			FileStatus[] fileStatuses = inputFileSystem.listStatus(inputPath);

			// define the schema that the resultant table will have: date, hour, pagename, pageviews
			final Schema tableSchema = new Schema("pagecounts",
			    Fields.parse("date:string, hour:string, pagename:string, pageviews:int"));
			// define the schema of the input files: projectcode, pagename, pageviews, bytes
			Schema fileSchema = new Schema("pagecountsfile",
			    Fields.parse("projectcode:string, pagename:string, pageviews:int, bytes:long"));

			// instantiate a TableBuilder
			TableBuilder tableBuilder = new TableBuilder(tableSchema);

			// for every input file...
			for(FileStatus fileStatus : fileStatuses) {
				String fileName = fileStatus.getPath().getName().toString();
				// strip the date and the hour from the file name
				String fileDate = fileName.split("-")[1];
				String fileHour = fileName.split("-")[2].substring(0, 2);
				// instantiate a custom RecordProcessor to process the records of this file
				PageCountsRecordProcessor recordProcessor = new PageCountsRecordProcessor(tableSchema, fileDate,
				    fileHour);
				// use the tableBuilder method for adding each of the files to the mix
				tableBuilder.addCSVTextFile(fileStatus.getPath(), ' ', TupleTextInputFormat.NO_QUOTE_CHARACTER,
				    TupleTextInputFormat.NO_ESCAPE_CHARACTER, false, false, TupleTextInputFormat.NO_NULL_STRING,
				    fileSchema, recordProcessor);
			}

			// partition the dataset by pagename - which should give a fair even distribution.
			tableBuilder.partitionBy("pagename");
			// create a compound index on pagename, date so that typical queries for the dataset will be fast
			tableBuilder.createIndex("pagename", "date");

			long nonExactPageSize = memoryForIndexing / 32000; // number of pages
			int pageSize = (int) Math.pow(2, (int) Math.round(Math.log(nonExactPageSize) / Math.log(2)));
			Log.info("Pagesize = " + pageSize + " as memory for indexing was [" + memoryForIndexing
			    + "] and there are 32000 pages.");

			tableBuilder.initialSQL("pragma page_size=" + pageSize);
			// insertion order is very important for optimizing query speed because it makes data be co-located in disk
			tableBuilder.insertionSortOrder(OrderBy.parse("pagename:asc, date:asc"));

			// instantiate a TablespaceBuilder
			TablespaceBuilder tablespaceBuilder = new TablespaceBuilder();

			// we will partition this dataset in as many partitions as:
			tablespaceBuilder.setNPartitions(nPartitions);
			tablespaceBuilder.add(tableBuilder.build());
			// we turn a specific SQLite pragma on for making autocomplete queries fast
			tablespaceBuilder.initStatements("pragma case_sensitive_like=true;");

			HadoopUtils.deleteIfExists(outFs, outPath);

			// finally, instantiate a TablespaceGenerator and execute it
			TablespaceGenerator tablespaceViewBuilder;

			if(generateTupleFiles) {
				// we subclass TablespaceGenerator to be able to run the generation without outputting the SQLite stores, for
				// benchmark comparisons.
				// In the future this feature may be useful in general for debugging store creation.
				tablespaceViewBuilder = new TablespaceGenerator(tablespaceBuilder.build(), outPath, this.getClass()) {

					@Override
					public void generateView(Configuration conf, SamplingType samplingType,
					    SamplingOptions samplingOptions) throws Exception {

						prepareOutput(conf);
						final int nPartitions = tablespace.getnPartitions();
						if(nPartitions > 1) {
							partitionMap = sample(nPartitions, conf, samplingType, samplingOptions);
						} else {
							partitionMap = PartitionMap.oneShardOpenedMap();
						}
						writeOutputMetadata(conf);

						TupleMRBuilder builder = createMRBuilder(nPartitions, conf);
						// Set a TupleOutput here instead of SQLiteOutput
						builder.setOutput(new Path(outputPath, OUT_STORE), new TupleOutputFormat(tableSchema),
						    ITuple.class, NullWritable.class);
						executeViewGeneration(builder);
					}
				};
			} else {
				// ... otherwise a standard TablespaceGenerator is used.
				tablespaceViewBuilder = new TablespaceGenerator(tablespaceBuilder.build(), outPath, this.getClass());
			}

			tablespaceViewBuilder.generateView(getConf(), SamplingType.FULL_SCAN,
			    new TupleSampler.RandomSamplingOptions());
		}

		if(deploy) {
			// use StoreDeployerTool for deploying the already generated dataset
			StoreDeployerTool deployer = new StoreDeployerTool(qnode, getConf());
			ArrayList<TablespaceDepSpec> deployments = new ArrayList<TablespaceDepSpec>();
			deployments.add(new TablespaceDepSpec("pagecounts", outPath.toString(), repFactor, null));
			deployer.deploy(deployments);
		}
		return 1;
	}

	public static final void main(String[] args) throws Exception {
		ToolRunner.run(new PageCountsExample(), args);
	}
}
