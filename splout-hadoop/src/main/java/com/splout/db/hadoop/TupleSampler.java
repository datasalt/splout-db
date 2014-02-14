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
import java.util.*;

import com.datasalt.pangool.tuplemr.mapred.lib.output.HadoopOutputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.mockito.Mockito;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.tuplemr.MapOnlyJobBuilder;
import com.datasalt.pangool.tuplemr.MultipleOutputsCollector;
import com.datasalt.pangool.tuplemr.mapred.MapOnlyMapper;
import com.datasalt.pangool.utils.TaskAttemptContextFactory;
import com.splout.db.common.PartitionMap;

/**
 * This class samples a list of {@link TableInput} files that produce a certain Table Schema. There are two sampling
 * methods supported:
 * <ul>
 * <li>FULL_SCAN: It uses a Map-Only Job for performing Reservoir Sampling over the whole dataset.</li>
 * <li>RANDOM: Inspired by Hadoop's TeraInputFormat. A Hadoop Job is not needed. Consecutive records are read from each
 * InputSplit. </li>
 * </ul>
 * Sampling can be used by {@link TablespaceGenerator} for determining a {@link PartitionMap} based on the approximated
 * distribution of the keys.
 */
@SuppressWarnings({ "serial", "rawtypes" })
public class TupleSampler implements Serializable {

	private final static Log logger = LogFactory.getLog(TupleSampler.class);

	private final SamplingType samplingType;
	private final SamplingOptions options;
	private Class callingClass;
	
	public enum SamplingType {
    FULL_SCAN, RANDOM
	}

	public static class TupleSamplerException extends Exception {

		public TupleSamplerException(String reason) {
			super(reason);
		}

		public TupleSamplerException(Exception e) {
			super(e);
		}

    public TupleSamplerException(String message, Throwable cause) {
      super(message, cause);
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

	// Options for RANDOM sampling
	public static class RandomSamplingOptions extends SamplingOptions {

		public RandomSamplingOptions() {
			super();
			setMaxSplitsToVisit(1000);
		}

		public int getMaxSplitsToVisit() {
			return (Integer) this.get("maxSplitsToVisit");
		}

		public void setMaxSplitsToVisit(int maxSplitsToVisit) {
			this.put("maxSplitsToVisit", maxSplitsToVisit);
		}
	}

	public TupleSampler(SamplingType samplingType, SamplingOptions options, Class callingClass) {
		this.samplingType = samplingType;
		this.options = options;
		this.callingClass = callingClass;
	}

  public long sample(TablespaceSpec tablespace, Configuration hadoopConf,
                     long sampleSize, Path outFile) throws TupleSamplerException {
    // 1 - Determine Input Splits
    // 2 - Launch sampling with the selected method
    // 3 - Recovering results
    List<InputSplit> splits = new ArrayList<InputSplit>();
    Map<InputSplit, InputFormat<ITuple, NullWritable>> splitToFormat = new HashMap<InputSplit, InputFormat<ITuple, NullWritable>>();
    Map<InputSplit, RecordProcessor> recordProcessorPerSplit = new HashMap<InputSplit, RecordProcessor>();
    Map<InputSplit, Map<String, String>> specificHadoopConfMap = new HashMap<InputSplit, Map<String, String>>();
    Map<InputSplit, TableSpec> splitToTableSpec = new HashMap<InputSplit, TableSpec>();
    Map<InputSplit, JavascriptEngine> splitToJsEngine = new HashMap<InputSplit, JavascriptEngine>();

    try {
      for(Table table : tablespace.getPartitionedTables()) {

        // Initialize JavaScript engine if needed
        JavascriptEngine jsEngine = null;
        TableSpec tableSpec = table.getTableSpec();
        if (tableSpec.getPartitionByJavaScript() != null) {
          try {
            jsEngine = new JavascriptEngine(tableSpec.getPartitionByJavaScript());
          } catch(Throwable e) {
            throw new RuntimeException(e);
          }
        }

        for(TableInput tableFile : table.getFiles()) {
          Job job = new Job(hadoopConf);
          FileInputFormat.setInputPaths(job, tableFile.getPaths());
          if(options.getMaxInputSplitSize() != null) {
            logger.info("Using max input split size: " + options.getMaxInputSplitSize());
            FileInputFormat.setMaxInputSplitSize(job, options.getMaxInputSplitSize());
          }
          job.setInputFormatClass(FileInputFormat.class);


          if(tableFile.getSpecificHadoopInputFormatContext() != null) {
            for(Map.Entry<String, String> specificHadoopConf : tableFile
                .getSpecificHadoopInputFormatContext().entrySet()) {
              job.getConfiguration().set(specificHadoopConf.getKey(), specificHadoopConf.getValue());
            }
          }

          for(InputSplit split : tableFile.getFormat().getSplits(job)) {
            if(tableFile.getSpecificHadoopInputFormatContext() != null) {
              specificHadoopConfMap.put(split, tableFile.getSpecificHadoopInputFormatContext());
            }
            splitToFormat.put(split, tableFile.getFormat());
            recordProcessorPerSplit.put(split, tableFile.getRecordProcessor());
            splitToTableSpec.put(split, tableSpec);
            splitToJsEngine.put(split, jsEngine);
            splits.add(split);
          }
        }
      }

      long retrievedSamples;
      if(samplingType.equals(SamplingType.RANDOM)) {
        try {
          RandomSamplingOptions defOptions = (RandomSamplingOptions) options;
          // Default sampling method
          retrievedSamples = randomSampling(
              sampleSize,
              hadoopConf,
              outFile,
              splits,
              splitToTableSpec,
              splitToFormat,
              specificHadoopConfMap,
              recordProcessorPerSplit,
              splitToJsEngine,
              defOptions.getMaxSplitsToVisit());
        } catch(ClassCastException ef) {
          throw new RuntimeException("Invalid options class: " + options.getClass() + " Expected:"
              + RandomSamplingOptions.class);
        }
      } else {
        // Reservoir sampling over full data
        retrievedSamples = fullScanSampling(
            tablespace, sampleSize, hadoopConf, outFile, splits.size());
      }
      return retrievedSamples;
    } catch (IOException e) {
      throw new TupleSamplerException(e);
    } catch (InterruptedException e) {
      throw new TupleSamplerException(e);
    }
  }


	/*
	 * Reservoir sampling that scans the full dataset to get the samples
	 * that are used for calculate the partition map. Based on
	 * http://en.wikipedia.org/wiki/Reservoir_sampling.
	 *
	 * Writes a SequenceFile with Text, NullWritable. The key
	 * contains the strings to be used for the partitioning.
	 *
	 * @return The number of samples retrieved
	 */
	private long fullScanSampling(TablespaceSpec tablespace, final long sampleSize, Configuration hadoopConf,
                                Path outputPath, final int nSplits) throws TupleSamplerException {

		MapOnlyJobBuilder builder = new MapOnlyJobBuilder(hadoopConf, "Reservoir Sampling to path " + outputPath);

    for(Table table : tablespace.getPartitionedTables()) {
      final TableSpec tableSpec = table.getTableSpec();
      final String getPartitionByJavaScript = tableSpec.getPartitionByJavaScript();
      for(TableInput inputFile : table.getFiles()) {
        final RecordProcessor processor = inputFile.getRecordProcessor();
        for(Path path : inputFile.getPaths()) {
          builder.addInput(path, inputFile.getFormat(),
              new MapOnlyMapper<ITuple, NullWritable, Text, NullWritable>() {

                final int nSamples = (int) (sampleSize / nSplits);
                final String[] samples = new String[nSamples];

                CounterInterface counterInterface;
                long recordCounter = 0;

                JavascriptEngine jsEngine = null;

                @Override
                protected void setup(Context context, MultipleOutputsCollector coll) throws IOException,
                    InterruptedException {
                  counterInterface = new CounterInterface(context);
                  // Initialize JavaScript engine if needed
                  if (getPartitionByJavaScript != null) {
                    try {
                      jsEngine = new JavascriptEngine(getPartitionByJavaScript);
                    } catch(Throwable e) {
                      throw new RuntimeException(e);
                    }
                  }
                };

                // Collect Tuples with decreasing probability
                // (http://en.wikipedia.org/wiki/Reservoir_sampling)
                protected void map(ITuple key, NullWritable value, Context context) throws IOException,
                    InterruptedException {
                  ITuple uTuple;
                  try {
                    uTuple = processor.process(key, counterInterface);
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
                    String pkey = null;
                    try {
                      pkey = TablespaceGenerator.getPartitionByKey(
                          uTuple,
                          tableSpec,
                          jsEngine);
                    } catch (Throwable e) {
                      throw new RuntimeException("Error when determining partition key.", e);
                    }
                    samples[(int) reservoirIndex] = pkey;
                  }

                  recordCounter++;
                }

                // Write the in-memory sampled Tuples
                protected void cleanup(Context context, MultipleOutputsCollector coll) throws IOException,
                    InterruptedException {
                  Text key = new Text();
                  for(String keyStr : samples) {
                    if(keyStr != null) {
                      key.set(keyStr);
                      context.write(key, NullWritable.get());
                    }
                  }
                }
              }, inputFile.getSpecificHadoopInputFormatContext());
        }
      }
    }
		// Set output path
		Path outReservoirPath = new Path(outputPath + "-reservoir");
		builder.setOutput(
        outReservoirPath,
        new HadoopOutputFormat(SequenceFileOutputFormat.class),
        Text.class,
        NullWritable.class);
		builder.setJarByClass(callingClass);

		try {
      Job job = null;
      job = builder.createJob();

      if(!job.waitForCompletion(true)) {
				throw new TupleSamplerException("Reservoir Sampling failed!");
			}
		} catch (Exception e) {
      throw new TupleSamplerException("Error creating or launching the sampling job.", e);
    }finally {
      try {
        builder.cleanUpInstanceFiles();
      } catch (IOException e) {
        throw new TupleSamplerException("Error cleaning up the sampling job.", e);
      }
    }

    long retrievedSamples = 0;
    try {
		  FileSystem outFs = outReservoirPath.getFileSystem(hadoopConf);
      if(outFs.listStatus(outReservoirPath) == null) {
        throw new IOException("Output folder not created: the Job failed!");
      }

      retrievedSamples = 0;
      // Instantiate the writer we will write samples to
      SequenceFile.Writer writer = new SequenceFile.Writer(outFs, hadoopConf, outputPath,
          Text.class, NullWritable.class);

      // Aggregate the output into a single file for being consistent with the other sampling methods
      for(FileStatus fileStatus : outFs.listStatus(outReservoirPath)) {
        Path thisPath = fileStatus.getPath();
        if(thisPath.getName().startsWith("part-m-")) {
          SequenceFile.Reader reader = new SequenceFile.Reader(outFs, thisPath, hadoopConf);
          Text key = new Text();
          while(reader.next(key)) {
            writer.append(key, NullWritable.get());
            retrievedSamples ++;
          }
          reader.close();
        }
      }

      writer.close();
      outFs.delete(outReservoirPath, true);
    } catch (IOException e) {
      throw new TupleSamplerException("Error consolidating the sample job results into one file.", e);
    }

    return retrievedSamples;
	}

	/**
	 * Random sampling method a-la-TeraSort, getting some consecutive samples from each InputSplit
	 * without using a Job.
	 * The output is SequenceFile with keys.
   *
   * @return The number of retrieved samples
	 */
	private long randomSampling(long sampleSize, Configuration hadoopConf,
                              Path outFile, List<InputSplit> splits,
                              Map<InputSplit, TableSpec> splitToTableSpec,
                              Map<InputSplit, InputFormat<ITuple, NullWritable>> splitToFormat,
                              Map<InputSplit, Map<String, String>> specificHadoopConf,
                              Map<InputSplit, RecordProcessor> recordProcessorPerSplit,
                              Map<InputSplit, JavascriptEngine> splitToJsEngine,
                              int maxSplitsToVisit) throws IOException {

		// Instantiate the writer we will write samples to
		FileSystem fs = FileSystem.get(outFile.toUri(), hadoopConf);

		if(splits.size() == 0) {
			throw new IllegalArgumentException("There are no splits to sample from!");
		}

    SequenceFile.Writer writer = new SequenceFile.Writer(fs, hadoopConf, outFile, Text.class, NullWritable.class);

		logger.info("Sequential sampling options, max splits to visit: " + maxSplitsToVisit
		    + ", samples to take: " + sampleSize + ", total number of splits: " + splits.size());
		int blocks = Math.min(maxSplitsToVisit, splits.size());
		blocks = Math.min((int) sampleSize, blocks);
		long recordsPerSample = sampleSize / blocks;
		int sampleStep = splits.size() / blocks;

		long records = 0;

		CounterInterface counterInterface = new CounterInterface(null) {

			public Counter getCounter(String group, String name) {
				return Mockito.mock(Counter.class);
			};
		};

		// Take N samples from different parts of the input
		for(int i = 0; i < blocks; ++i) {
			TaskAttemptID attemptId = new TaskAttemptID(new TaskID(), 1);

      TaskAttemptContext attemptContext = null;
      try {
        attemptContext = TaskAttemptContextFactory.get(hadoopConf, attemptId);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      InputSplit split = splits.get(sampleStep * i);
			if(specificHadoopConf.get(split) != null) {
				for(Map.Entry<String, String> specificConf : specificHadoopConf.get(split).entrySet()) {
					attemptContext.getConfiguration().set(specificConf.getKey(), specificConf.getValue());
				}
			}
			logger.info("Sampling split: " + split);
      RecordReader<ITuple, NullWritable> reader = null;
      try {
        reader = splitToFormat.get(split).createRecordReader(split,
            attemptContext);
        reader.initialize(split, attemptContext);

        RecordProcessor processor = recordProcessorPerSplit.get(split);
        Text key = new Text();
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
            try {
              key.set(TablespaceGenerator.getPartitionByKey(
                  uTuple,
                  splitToTableSpec.get(split),
                  splitToJsEngine.get(split)));
            } catch (Throwable e) {
              throw new RuntimeException("Error when determining partition key.", e);
            }

            writer.append(key, NullWritable.get());
            records += 1;
            if((i + 1) * recordsPerSample <= records) {
              break;
            }
          }
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

    }

		writer.close();
    return records;
	}
}