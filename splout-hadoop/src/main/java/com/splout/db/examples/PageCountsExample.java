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

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.tuplemr.mapred.lib.input.TupleTextInputFormat;
import com.datasalt.pangool.utils.HadoopUtils;
import com.splout.db.hadoop.StoreDeployerTool;
import com.splout.db.hadoop.TableBuilder;
import com.splout.db.hadoop.TablespaceBuilder;
import com.splout.db.hadoop.TablespaceDepSpec;
import com.splout.db.hadoop.TablespaceGenerator;
import com.splout.db.hadoop.TupleSampler;
import com.splout.db.hadoop.TupleSampler.SamplingType;

/**
 * An advanced Splout example with the Wikipedia pagecounts dataset:
 *  http://dom.as/2007/12/10/wikipedia-page-counters/
 */
public class PageCountsExample implements Tool {

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
		Path outPath = new Path(outputPath);
		FileSystem outFs = outPath.getFileSystem(getConf());

		if(generate) {
			Path inputPath = new Path(this.inputPath);
			FileSystem inputFileSystem = inputPath.getFileSystem(conf);

			FileStatus[] fileStatuses = inputFileSystem.listStatus(inputPath);

			Schema tableSchema = new Schema("pagecounts",
			    Fields.parse("date:string, hour:string, pagename:string, pageviews:int"));
			Schema fileSchema = new Schema("pagecountsfile",
			    Fields.parse("projectcode:string, pagename:string, pageviews:int, bytes:long"));

			TableBuilder tableBuilder = new TableBuilder(tableSchema);

			for(FileStatus fileStatus : fileStatuses) {
				String fileName = fileStatus.getPath().getName().toString();
				String fileDate = fileName.split("-")[1];
				String fileHour = fileName.split("-")[2].substring(0, 2);
				PageCountsRecordProcessor recordProcessor = new PageCountsRecordProcessor(tableSchema, fileDate,
				    fileHour);
				tableBuilder.addCSVTextFile(fileStatus.getPath(), ' ', TupleTextInputFormat.NO_QUOTE_CHARACTER,
				    TupleTextInputFormat.NO_ESCAPE_CHARACTER, false, false, TupleTextInputFormat.NO_NULL_STRING,
				    fileSchema, recordProcessor);
			}

			// Partition by the first two characters of the page so we can easily implement auto suggest
//			tableBuilder
//			    .partitionByJavaScript("function partition(record) { var str = record.get('pagename').toString(); if(str.length() > 2) { return str.substring(0, 2); } else { return str; } }");
			tableBuilder.partitionBy("pagename");
			tableBuilder.createIndex("pagename", "date");

			TablespaceBuilder tablespaceBuilder = new TablespaceBuilder();

			tablespaceBuilder.setNPartitions(nPartitions);
			tablespaceBuilder.add(tableBuilder.build());
			tablespaceBuilder.initStatements("pragma encoding=\"UTF-8\";", "pragma case_sensitive_like=true;");

			HadoopUtils.deleteIfExists(outFs, outPath);

			TablespaceGenerator tablespaceViewBuilder = new TablespaceGenerator(tablespaceBuilder.build(),
			    outPath);
			tablespaceViewBuilder.generateView(getConf(), SamplingType.RESERVOIR, new TupleSampler.DefaultSamplingOptions());
		}

		if(deploy) {
			StoreDeployerTool deployer = new StoreDeployerTool(qnode, getConf());
			ArrayList<TablespaceDepSpec> deployments = new ArrayList<TablespaceDepSpec>();
			deployments.add(new TablespaceDepSpec("pagecounts", outPath.toString(), repFactor));
			deployer.deploy(deployments);
		}
		return 1;
	}

	public static final void main(String[] args) throws Exception {
		ToolRunner.run(new PageCountsExample(), args);
	}
}
