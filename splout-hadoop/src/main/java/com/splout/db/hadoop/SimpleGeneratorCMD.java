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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.tuplemr.mapred.lib.input.TupleTextInputFormat;
import com.datasalt.pangool.utils.HadoopUtils;
import com.splout.db.common.SploutHadoopConfiguration;
import com.splout.db.hadoop.TupleSampler.SamplingType;

/**
 * This tool can be used for creating and deploying a single-table tablespace from text files. Use command-line
 * parameters for definiting your Tablespace. For a more general multi-tablespace generator use {@link GeneratorCMD}.
 */
public class SimpleGeneratorCMD implements Tool {

	private final static Log log = LogFactory.getLog(SimpleGeneratorCMD.class);

	@Parameter(required = true, names = { "-tb", "--tablespace" }, description = "The tablespace name.")
	private String tablespace;

	@Parameter(required = true, names = { "-t", "--table" }, description = "The table name.")
	private String tablename;

	@Parameter(required = true, names = { "-i", "--input" }, description = "Input path/glob containing data to use. If you are running the process from Hadoop, relative paths would use the Hadoop filesystem. Use full qualified URIs instead if you want other behaviour.")
	private String input;

	@Parameter(required = true, names = { "-o", "--output" }, description = "Output path where the view will be saved. If you are running the process from Hadoop, relative paths would use the Hadoop filesystem. Use full qualified URIs instead if you want other behaviour.")
	private String output;

	@Parameter(required = true, names = { "-s", "--schema" }, description = "Input schema, in Pangool in-line format. Fields are comma-sepparated. Example: \"id:int,name:string,salary:long\"")
	private String schema;

	@Parameter(required = true, names = { "-pby", "--partitionby" }, description = "The field or fields to partition the table by. Comma-sepparated if there is more than one.")
	private String partitionByFields;

	@Parameter(names = { "-idx", "--index" }, description = "The additional fields to index - (fields used to partitioning are always indexed). More than one allowed. Comma-sepparated in the case of a multifield index.")
	private List<String> indexes = new ArrayList<String>();

	@Parameter(required = true, names = { "-p", "--partitions" }, description = "The number of partitions to create for the view.")
	private Integer nPartitions;

	@Parameter(converter = CharConverter.class, names = { "-sep", "--separator" }, description = "The separator character of your text input file, defaults to a tabulation")
	private Character separator = '\t';

	@Parameter(converter = CharConverter.class, names = { "-quo", "--quotes" }, description = "The quotes character of your input file, defaults to none.")
	private Character quotes = TupleTextInputFormat.NO_QUOTE_CHARACTER;

	@Parameter(converter = CharConverter.class, names = { "-esc", "--escape" }, description = "The escape character of your input file, defaults to none.")
	private Character escape = TupleTextInputFormat.NO_ESCAPE_CHARACTER;

	@Parameter(names = { "-sh", "--skipheading" }, description = "Specify this flag for skipping the header line of your text file.")
	private boolean skipHeading = false;

	@Parameter(names = { "-sq", "--strictquotes" }, description = "Activate strict quotes mode where all values that are not quoted are considered null.")
	private boolean strictQuotes = false;

	@Parameter(names = { "-ns", "--nullstring" }, description = "A string sequence which, if found and not quoted, it's considered null. For example, \\N in mysqldumps.")
	private String nullString = TupleTextInputFormat.NO_NULL_STRING;

	@Parameter(names = { "-fw", "--fixedwidthfields" }, description = "When used, you must provide a comma-separated list of numbers. These numbers will be interpreted by pairs, as [beginning, end] inclusive position offsets. For example: 0,3,5,7 means there are two fields, the first one of 4 characters at offsets [0, 3] and the second one of 3 characters at offsets [5, 7]. This option can be used in combination with --nullstring parameter. The rest of CSV parameters are ignored.")
	private String fixedWidthFields;

	public static class CharConverter implements IStringConverter<Character> {

		@Override
		public Character convert(String value) {
			// Because space gets trimmed by JCommander we recover it here
			return value.length() == 0 ? ' ' : value.toCharArray()[0];
		}
	}

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
		jComm.setProgramName("Splout Tablespace View Generator");
		try {
			jComm.parse(args);
		} catch(Throwable t) {
			t.printStackTrace();
			jComm.usage();
			return -1;
		}

		log.info("Parsing input parameters...");

		Schema schema = new Schema(tablename, Fields.parse(this.schema));

		Path inputPath = new Path(input);
		Path out = new Path(output, tablespace);
		FileSystem outFs = out.getFileSystem(getConf());
		HadoopUtils.deleteIfExists(outFs, out);

		if(!FileSystem.getLocal(conf).equals(FileSystem.get(conf))) {
			File nativeLibs = new File("native");
			if(nativeLibs.exists()) {
				SploutHadoopConfiguration.addSQLite4JavaNativeLibsToDC(conf);
			}
		}

		int fixedWidth[] = null;
		if(fixedWidthFields != null) {
			String[] posStr = fixedWidthFields.split(",");
			fixedWidth = new int[posStr.length];
			for(int i = 0; i < posStr.length; i++) {
				try {
					fixedWidth[i] = new Integer(posStr[i]);
				} catch(NumberFormatException e) {
					System.out.println("Wrongly formed fixed with field list: [" + fixedWidthFields + "]. "
					    + posStr[i] + " does not look as a number.");
				}
			}
		}

		TablespaceBuilder builder = new TablespaceBuilder();
		TableBuilder tableBuilder = new TableBuilder(schema);
		if(fixedWidth == null) {
			// CSV
			tableBuilder.addCSVTextFile(inputPath, separator, quotes, escape, skipHeading, strictQuotes,
			    nullString);
		} else {
			// Fixed Width
			tableBuilder.addFixedWidthTextFile(inputPath, schema, fixedWidth, skipHeading, nullString, null);
		}

		String[] strPartitionByFields = this.partitionByFields.split(",");
		tableBuilder.partitionBy(strPartitionByFields);

		for(String strIndex : indexes) {
			tableBuilder.createIndex(strIndex.split(","));
		}

		log.info("Generating view with Hadoop...");

		builder.add(tableBuilder.build());
		builder.setNPartitions(nPartitions);

		TablespaceGenerator viewGenerator = new TablespaceGenerator(builder.build(), out, this.getClass());
		viewGenerator.generateView(conf, SamplingType.DEFAULT, new TupleSampler.DefaultSamplingOptions()); 

		log.info("Success! Tablespace [" + tablespace + "] with table [" + tablename
		    + "] properly created at path [" + out + "]");
		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new SimpleGeneratorCMD(), args);
	}
}