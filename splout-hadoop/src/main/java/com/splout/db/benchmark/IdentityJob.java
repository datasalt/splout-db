package com.splout.db.benchmark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.tuplemr.IdentityTupleMapper;
import com.datasalt.pangool.tuplemr.IdentityTupleReducer;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.mapred.lib.input.TupleTextInputFormat;
import com.datasalt.pangool.tuplemr.mapred.lib.input.TupleTextInputFormat.FieldSelector;
import com.datasalt.pangool.tuplemr.mapred.lib.output.TupleTextOutputFormat;
import com.datasalt.pangool.utils.HadoopUtils;

/**
 * This Job implements a identity Map/Reduce in two ways: one using the plain Hadoop API and the other one using the Pangool API
 * for parsing CSV files. With this Job we can measure 1) The overhead of using Splout store generator tools against a plain Indetity Hadoop Job
 * and 2) Which part of this overhead is only due to parsing CSV files with Pangool. 
 */
public class IdentityJob implements Tool {

	@Parameter(required = true, names = { "-i", "--inputpath" }, description = "The input path for the identity Job. Must be textual files.")
	private String inputPath;

	@Parameter(required = true, names = { "-o", "--outputpath" }, description = "The output path for the identity Job.")
	private String outputPath;

	@Parameter(required = false, names = { "-ps", "--pangoolSchema" }, description = "Provide a Pangool-schema and Pangool will be used for parsing the input text file into a Tuple. Using this option one can measure the overhead of using Pangool's textual input format.")
	private String pangoolSchema = null;

	@Parameter(required = false, names = { "-gb", "--groupBy" }, description = "If pangoolSchema is provided, a groupBy clause must be provided too. Use a field in your schema that makes the data as evenly spread across reducers as possible.")
	private String groupBy = null;

	// Basic CSV parsing parameters, optionally used if pangoolSchema != null, can be overrided
	// --------------------------------//
	@Parameter(names = { "-sep", "--separator" }, description = "The separator character of your text input file, defaults to a space.")
	private String separator = " ";

	@Parameter(names = { "-quo", "--quotes" }, description = "The quotes character of your input file, defaults to none.")
	private String quotes = TupleTextInputFormat.NO_QUOTE_CHARACTER + "";

	@Parameter(names = { "-esc", "--escape" }, description = "The escape character of your input file, defaults to none.")
	private String escape = TupleTextInputFormat.NO_ESCAPE_CHARACTER + "";

	@Parameter(names = { "-sh", "--skipheading" }, description = "Specify this flag for skipping the header line of your text file.")
	private boolean skipHeading = false;
	// --------------------------------//

	private Configuration conf;

	@Override
	public Configuration getConf() {
		return null;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public int run(String[] params) throws Exception {
		// Validate params etc
		JCommander jComm = new JCommander(this);
		jComm.setProgramName("Identity Job");
		try {
			jComm.parse(params);
		} catch(ParameterException e) {
			System.err.println(e.getMessage());
			jComm.usage();
			System.exit(-1);
		}

		Path outP = new Path(outputPath);
		HadoopUtils.deleteIfExists(FileSystem.get(conf), outP);

		if(pangoolSchema == null) {
			// Use plain Hadoop API
			Job job = new Job(conf);
			job.setInputFormatClass(TextInputFormat.class);
			FileInputFormat.setInputPaths(job, inputPath);
			FileOutputFormat.setOutputPath(job, outP);

			job.waitForCompletion(true);

		} else {
			if(groupBy == null) {
				System.err.println("If pangoolSchema is used, groupBy must also be used.");
				jComm.usage();
				System.exit(-1);
			}

			Schema schema = new Schema("sch", Fields.parse(pangoolSchema));
			Path inputP = new Path(inputPath);

			// Use Pangool API - parse CSV, etc
			TupleMRBuilder builder = new TupleMRBuilder(conf);
			TupleTextInputFormat parsingInputFormat = new TupleTextInputFormat(schema, skipHeading, false,
			    separator.charAt(0), quotes.charAt(0), escape.charAt(0), FieldSelector.NONE, null);
			TupleTextOutputFormat outputFormat = new TupleTextOutputFormat(schema, false, separator.charAt(0),
			    quotes.charAt(0), escape.charAt(0));

			builder.addIntermediateSchema(schema);
			builder.addInput(inputP, parsingInputFormat, new IdentityTupleMapper());
			builder.setGroupByFields(groupBy);
			builder.setOutput(outP, outputFormat, ITuple.class, NullWritable.class);
			builder.setTupleReducer(new IdentityTupleReducer());
			builder.setJarByClass(this.getClass());
			
			builder.createJob().waitForCompletion(true);
		}

		return 1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new IdentityJob(), args);
	}
}
