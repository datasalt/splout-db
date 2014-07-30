package com.splout.db.hadoop;

/*
 * #%L
 * Splout SQL Hadoop library
 * %%
 * Copyright (C) 2012 - 2013 Datasalt Systems S.L.
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

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.utils.TaskAttemptContextFactory;

/**
 * Small piece of code that can sample a Pangool Schema from an arbitrary InputFormat<ITuple, NullWritable>. It just
 * reads the first Tuple and reads the Schema from it.
 */
public class SchemaSampler {

	private final static Log log = LogFactory.getLog(SchemaSampler.class);

	public static Schema sample(Configuration conf, Path input,
	    InputFormat<ITuple, NullWritable> inputFormat) throws IOException, InterruptedException {
		Schema schema = null;

		// sample schema from input path given the provided InputFormat
		Job job = new Job(conf);
		FileInputFormat.setInputPaths(job, input);
		// get first inputSplit
		List<InputSplit> inputSplits = inputFormat.getSplits(job);
		if(inputSplits == null || inputSplits.size() == 0) {
			throw new IOException(
			    "Given input format doesn't produce any input split. Can't sample first record. PATH: " + input);
		}
		InputSplit inputSplit = inputSplits.get(0);
		TaskAttemptID attemptId = new TaskAttemptID(new TaskID(), 1);
		TaskAttemptContext attemptContext;
		try {
			attemptContext = TaskAttemptContextFactory.get(conf, attemptId);
		} catch(Exception e) {
			throw new IOException(e);
		}

		RecordReader<ITuple, NullWritable> rReader = inputFormat.createRecordReader(inputSplit,
		    attemptContext);
		rReader.initialize(inputSplit, attemptContext);

		if(!rReader.nextKeyValue()) {
			throw new IOException("Can't read first record of first input split of the given path [" + input
			    + "].");
		}

		// finally get the sample schema
		schema = rReader.getCurrentKey().getSchema();
		log.info("Sampled schema from [" + input + "] : " + schema);
		rReader.close();

		return schema;
	}
}
