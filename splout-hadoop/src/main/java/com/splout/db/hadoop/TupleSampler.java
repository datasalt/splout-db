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

import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.mockito.Mockito;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.tuplemr.MapOnlyJobBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.mapred.MapOnlyMapper;
import com.datasalt.pangool.tuplemr.mapred.lib.input.TupleInputFormat.TupleInputReader;
import com.datasalt.pangool.tuplemr.mapred.lib.output.TupleOutputFormat.TupleRecordWriter;
import com.splout.db.common.PartitionMap;

/**
 * This class samples a list of {@link TableInput} files that produce a certain Table Schema. There are two sampling
 * methods supported:
 * <ul>
 * <li>DEFAULT: Inspired by Hadoop's TeraInputFormat. A Hadoop Job is not needed. Consecutive records are read from each
 * InputSplit.</li>
 * <li>RESERVOIR: It uses a Map-Only Pangool Job for performing Reservoir Sampling over the dataset.</li>
 * </ul>
 * Sampling can be used by {@link TablespaceGenerator} for determining a {@link PartitionMap} based on the approximated
 * distribution of the keys.
 */
@SuppressWarnings("serial")
public class TupleSampler implements Serializable {

	private final static Log logger = LogFactory.getLog(TupleSampler.class);

	private final SamplingType samplingType;
	private final SamplingOptions options;

	public enum SamplingType {
		DEFAULT, RESERVOIR
	}

	public static class TupleSamplerException extends Exception {

		public TupleSamplerException(Exception e) {
			super(e);
		}
	}

	// Each sampling algorithm may have its own options but there are some which are common to both
	public static abstract class SamplingOptions extends HashMap<String, Object> {

		public Long getMaxInputSplitSize() {
			return (Long) this.get("maxInputSplitSize");
		}

		public void setMaxInputSplitSize(Long maxInputSplitSize) {
			this.put("maxInputSplitSize", maxInputSplitSize);
		}
	}

	// Options for DEFAULT sampling
	public static class DefaultSamplingOptions extends SamplingOptions {
	}

	public TupleSampler(SamplingType samplingType, SamplingOptions options) {
		this.samplingType = samplingType;
		this.options = options;
	}

	public void sample(List<TableInput> inputFiles, Schema tableSchema, Configuration hadoopConf,
	    long sampleSize, Path outFile) throws TupleSamplerException {

		try {
			List<InputSplit> splits = new ArrayList<InputSplit>();
			Map<InputSplit, InputFormat<ITuple, NullWritable>> splitToFormat = new HashMap<InputSplit, InputFormat<ITuple, NullWritable>>();
			Map<InputSplit, RecordProcessor> recordProcessorPerSplit = new HashMap<InputSplit, RecordProcessor>();

			// Iterate over all {@link TableInput} and collect information about the InputSplits derived from them
			for(TableInput tableFile : inputFiles) {
				Job job = new Job(hadoopConf);
				FileInputFormat.setInputPaths(job, tableFile.getPaths());
				if(options.getMaxInputSplitSize() != null) {
					logger.info("Using max input split size: " + options.getMaxInputSplitSize());
					FileInputFormat.setMaxInputSplitSize(job, options.getMaxInputSplitSize());
				}
				job.setInputFormatClass(FileInputFormat.class);
				
				for(InputSplit split : tableFile.getFormat().getSplits(job)) {
					splitToFormat.put(split, tableFile.getFormat());
					recordProcessorPerSplit.put(split, tableFile.getRecordProcessor());
					splits.add(split);
				}
			}

			FileSystem outFs = outFile.getFileSystem(hadoopConf);
			if(outFs.exists(outFile)) {
				outFs.delete(outFile, false);
			}

			if(samplingType.equals(SamplingType.DEFAULT)) {
				// Default sampling method
				defaultSampling(tableSchema, sampleSize, hadoopConf, outFile, splits, splitToFormat,
				    recordProcessorPerSplit);
			} else {
				// Reservoir sampling
				reservoirSampling(tableSchema, sampleSize, hadoopConf, outFile, splits.size(), inputFiles);
			}
		} catch(Exception e) {
			throw new TupleSamplerException(e);
		}
	}

	/*
	 * Reservoir sampling, to be used in datasets where default method is not enough.
	 */
	private void reservoirSampling(Schema tableSchema, final long sampleSize, Configuration hadoopConf,
	    Path outputPath, final int nSplits, List<TableInput> inputFiles) throws IOException,
	    InterruptedException, ClassNotFoundException, TupleMRException, URISyntaxException {

		MapOnlyJobBuilder builder = new MapOnlyJobBuilder(hadoopConf);
		for(TableInput inputFile : inputFiles) {
			final RecordProcessor processor = inputFile.getRecordProcessor();
			for(Path path : inputFile.getPaths()) {
				builder.addInput(path, inputFile.getFormat(),
				    new MapOnlyMapper<ITuple, NullWritable, ITuple, NullWritable>() {

					    final int nSamples = (int) (sampleSize / nSplits);
					    final ITuple[] samples = new ITuple[nSamples];

					    CounterInterface counterInterface;
					    long recordCounter = 0;

					    protected void setup(Context context) throws IOException, InterruptedException {
						    counterInterface = new CounterInterface(context);
					    };

					    // Collect Tuples with decreasing probability
					    // (http://en.wikipedia.org/wiki/Reservoir_sampling)
					    protected void map(ITuple key, NullWritable value, Context context) throws IOException,
					        InterruptedException {
						    ITuple uTuple;
						    try {
							    uTuple = processor.process(key,
							        counterInterface);
						    } catch(Throwable e) {
							    throw new RuntimeException(e);
						    }
						    if(uTuple == null) { // user may have filtered the record
							    return;
						    }

						    long reservoirIndex;
						    if(recordCounter < nSamples) {
							    reservoirIndex = recordCounter;
						    } else {
							    reservoirIndex = (long) (Math.random() * recordCounter);
						    }

						    if(reservoirIndex < nSamples) {
							    samples[(int) reservoirIndex] = new NullableTuple(uTuple, true); // deep copy the Tuple
						    }

						    recordCounter++;
					    }

					    // Write the in-memory sampled Tuples
					    protected void cleanup(Context context) throws IOException, InterruptedException {
						    for(ITuple tuple : samples) {
							    if(tuple != null) {
								    context.write(tuple, NullWritable.get());
							    }
						    }
					    }
				    });
			}
		}
		// Set output path
		Path outReservoirPath = new Path(outputPath + "-reservoir");
		builder.setTupleOutput(outReservoirPath, new NullableSchema(tableSchema));
		builder.createJob().waitForCompletion(true);

		FileSystem outFs = outReservoirPath.getFileSystem(hadoopConf);
		// Instantiate the writer we will write samples to
		TupleRecordWriter writer = TupleRecordWriter.getTupleWriter(hadoopConf, new NullableSchema(
		    tableSchema), outputPath);

		if(outFs.listStatus(outReservoirPath) == null) {
			throw new IOException("Output folder not created: the Job failed!");
		}
		
		// Aggregate the output into a single file for being consistent with the other sampling methods
		for(FileStatus fileStatus : outFs.listStatus(outReservoirPath)) {
			Path thisPath = fileStatus.getPath();
			if(thisPath.getName().startsWith("part-m-")) {
				TupleInputReader reader = new TupleInputReader(hadoopConf);
				reader.initialize(thisPath, hadoopConf);
				while(reader.nextKeyValueNoSync()) {
					writer.write(reader.getCurrentKey(), NullWritable.get());
				}
				reader.close();
			}
		}

		writer.close(null);
		outFs.delete(outReservoirPath, true);
	}

	/*
	 * Default sampling method a-la-TeraSort, getting some consecutive samples from each InputSplit.
	 */
	private void defaultSampling(Schema tableSchema, long sampleSize, Configuration hadoopConf,
	    Path outFile, List<InputSplit> splits,
	    Map<InputSplit, InputFormat<ITuple, NullWritable>> splitToFormat,
	    Map<InputSplit, RecordProcessor> recordProcessorPerSplit) throws IOException, InterruptedException {

		// Instantiate the writer we will write samples to
		TupleRecordWriter writer = TupleRecordWriter.getTupleWriter(hadoopConf, new NullableSchema(
		    tableSchema), outFile);

		if(splits.size() == 0) {
			throw new IllegalArgumentException("There are nos plits to sample from!");
		}
		logger.info("Sampling from input splits > " + splits);
		int samples = Math.min(10, splits.size());
		long recordsPerSample = sampleSize / samples;
		int sampleStep = splits.size() / samples;

		long records = 0;

		CounterInterface counterInterface = new CounterInterface(null) {

			public Counter getCounter(String group, String name) {
				return Mockito.mock(Counter.class);
			};
		};

		// Take N samples from different parts of the input
		for(int i = 0; i < samples; ++i) {
			TaskAttemptID attemptId = new TaskAttemptID(new TaskID(), 1);
			TaskAttemptContext attemptContext = new TaskAttemptContext(hadoopConf, attemptId);
			InputSplit split = splits.get(sampleStep * i);
			logger.info("Sampling split: " + split);
			RecordReader<ITuple, NullWritable> reader = splitToFormat.get(split).createRecordReader(split,
			    attemptContext);
			reader.initialize(split, attemptContext);
			RecordProcessor processor = recordProcessorPerSplit.get(split);
			while(reader.nextKeyValue()) {
				//
				ITuple tuple = reader.getCurrentKey();

				ITuple uTuple;
				try {
					uTuple = processor.process(tuple, counterInterface);
				} catch(Throwable e) {
					throw new RuntimeException(e);
				}
				if(uTuple != null) { // user may have filtered the record
					writer.write(new NullableTuple(uTuple), NullWritable.get());
					records += 1;
					if((i + 1) * recordsPerSample <= records) {
						break;
					}
				}
			}
		}

		writer.close(null);
	}
}