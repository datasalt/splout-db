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

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.io.TupleFile;
import com.datasalt.pangool.tuplemr.Criteria.Order;
import com.datasalt.pangool.tuplemr.Criteria.SortElement;
import com.datasalt.pangool.tuplemr.*;
import com.splout.db.common.JSONSerDe;
import com.splout.db.common.JSONSerDe.JSONSerDeException;
import com.splout.db.common.PartitionEntry;
import com.splout.db.common.PartitionMap;
import com.splout.db.common.Tablespace;
import com.splout.db.hadoop.TupleSQLite4JavaOutputFormat.TupleSQLiteOutputFormatException;
import com.splout.db.hadoop.TupleSampler.TupleSamplerException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.mortbay.log.Log;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A process that generates the SQL data stores needed for deploying a tablespace in Splout, giving a file set table
 * specification as input.
 * <p>
 * The input to this process will be:
 * <ul>
 * <li>The {@link Tablespace} specification.</li>
 * <li>The Hadoop output Path</li>
 * </ul>
 * The output of the process is a Splout deployable path with a {@link PartitionMap} . The format of the output is:
 * outputPath + / + {@link #OUT_PARTITION_MAP} for the partition map, outputPath + / + {@link #OUT_SAMPLED_INPUT} for
 * the list of sampled keys and outputPath + / + {@link #OUT_STORE} for the folder containing the generated SQL store.
 * <p>
 * For creating the store we first sample the input dataset with {@link TupleSampler} and then execute a Hadoop job that
 * distributes the data accordingly. The Hadoop job will use {@link TupleSQLite4JavaOutputFormat}.
 */
@SuppressWarnings({ "serial", "rawtypes" })
public class TablespaceGenerator implements Serializable {

	public static class TablespaceGeneratorException extends Exception {

		public TablespaceGeneratorException() {
			super();
		}

		public TablespaceGeneratorException(String message, Throwable cause) {
			super(message, cause);
		}

		public TablespaceGeneratorException(String message) {
			super(message);
		}

		public TablespaceGeneratorException(Throwable cause) {
			super(cause);
		}

	}

	// --- Input parameters --- //
	private transient final Path outputPath;
	protected transient final TablespaceSpec tablespace;

	// Number of keys that will be sampled for partitioning
	private int recordsToSample = 100000;
	// Number of SQL statements to execute before a COMMIT
	private int batchSize = 1000000;
	protected PartitionMap partitionMap;

	private TupleReducer<ITuple, NullWritable> customReducer = null;

	public TablespaceGenerator(TablespaceSpec tablespace, Path outputPath) {
		this.tablespace = tablespace;
		this.outputPath = outputPath;
	}

	public final static String OUT_SAMPLED_INPUT = "sampled-input";
	public final static String OUT_PARTITION_MAP = "partition-map";
	public final static String OUT_INIT_STATEMENTS = "init-statements";
	public final static String OUT_STORE = "store";

	/**
	 * This is the public method which has to be called when using this class as an API.
	 * Business logic has been split in various protected functions to ease understading of it and also to be able
	 * to subclass this easily to extend its functionality.
	 */
	public void generateView(Configuration conf, TupleSampler.SamplingType samplingType,
	    TupleSampler.SamplingOptions samplingOptions) throws Exception {

		prepareOutput(conf);

		final int nPartitions = tablespace.getnPartitions();

		if(nPartitions > 1) {
			partitionMap = sample(nPartitions, conf, samplingType, samplingOptions);
		} else {
			partitionMap = PartitionMap.oneShardOpenedMap();
		}

		writeOutputMetadata(conf);

		Job job = createMRBuilder(nPartitions, conf).createJob();
		executeViewGeneration(job);
	}

	// ------------------------------- //
	
	protected void prepareOutput(Configuration conf) throws IOException {
		FileSystem fileSystem = outputPath.getFileSystem(conf);
		fileSystem.mkdirs(outputPath);
	}
	
	/**
	 * Write the partition map and other metadata to the output folder. They will be needed for deploying the dataset to Splout.
	 */
	protected void writeOutputMetadata(Configuration conf) throws IOException, JSONSerDeException {
		FileSystem fileSystem = outputPath.getFileSystem(conf);
		
		Path partitionMapPath = new Path(outputPath, OUT_PARTITION_MAP);
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fileSystem.create(
		    partitionMapPath, true)));
		writer.write(JSONSerDe.ser(partitionMap));
		writer.close();
		
		// Write init statements, if applicable
		if(tablespace.getInitStatements() != null) {
			Path initStatementsPath = new Path(outputPath, OUT_INIT_STATEMENTS);
			writer = new BufferedWriter(new OutputStreamWriter(fileSystem.create(initStatementsPath, true)));
			writer.write(JSONSerDe.ser(tablespace.getInitStatements()));
			writer.close();
		}
	}
	
	/**
	 * Returns the partition key either by using partition-by-fields or partition-by-javascript as configured in the Table
	 * Spec.
	 */
	protected static String getPartitionByKey(ITuple tuple, TableSpec tableSpec, JavascriptEngine jsEngine)
	    throws Throwable {
		
		String strKey = "";
		if(tableSpec.getPartitionFields() != null) {
			for(Field partitionField : tableSpec.getPartitionFields()) {
				Object obj = tuple.get(partitionField.getName());
				if(obj == null) {
					strKey += "";
				} else {
					strKey += obj.toString();
				}
			}
		} else {
			// use JavaScript
			strKey = jsEngine.execute("partition", tuple);
			if(strKey == null) {
				strKey = "";
			}
		}
		return strKey;
	}

	/**
	 * Samples the input, if needed.
	 */
	protected PartitionMap sample(int nPartitions, Configuration conf, TupleSampler.SamplingType samplingType,
	    TupleSampler.SamplingOptions samplingOptions) throws TupleSamplerException, IOException {
		
		FileSystem fileSystem = outputPath.getFileSystem(conf);
		List<String> keys = new ArrayList<String>();

		// We must sample as many tables as there are for the tablespace
		for(Table table : tablespace.getPartitionedTables()) {
			JavascriptEngine jsEngine = null;
			if(table.getTableSpec().getPartitionByJavaScript() != null) {
				try {
					jsEngine = new JavascriptEngine(table.getTableSpec().getPartitionByJavaScript());
				} catch(Throwable e) {
					throw new RuntimeException(e);
				}
			}

			Path sampledInput = new Path(outputPath, OUT_SAMPLED_INPUT);
			TupleSampler sampler = new TupleSampler(samplingType, samplingOptions);
			sampler.sample(table.getFiles(), table.getTableSpec().getSchema(), conf, recordsToSample,
			    sampledInput);

			// 1.1 Read sampled keys
			TupleFile.Reader reader = new TupleFile.Reader(fileSystem, conf, sampledInput);
			Tuple tuple = new Tuple(reader.getSchema());

			while(reader.next(tuple)) {
				String key;
				try {
					key = getPartitionByKey(tuple, table.getTableSpec(), jsEngine);
				} catch(Throwable e) {
					throw new RuntimeException(e);
				}
				keys.add(key);
			}
			reader.close();
		}

		// 1.2 Sort them using default comparators
		Collections.sort(keys);

		/*
		 * 2: Calculate partition map
		 */
		// 2.1 Select "n" keys evenly distributed
		List<PartitionEntry> partitionEntries = new ArrayList<PartitionEntry>();
		int offset = keys.size() / nPartitions;
		String min = null;
		for(int i = 0; i < nPartitions; i++) {
			PartitionEntry entry = new PartitionEntry();
			if(min != null) {
				entry.setMin(min);
			}
			int keyIndex = (i + 1) * offset;
			if(keyIndex < keys.size()) {
				entry.setMax(keys.get(keyIndex));
			}
			min = entry.getMax();
			entry.setShard(i);
			partitionEntries.add(entry);
		}
		// Leave the last entry opened for having a complete map
		partitionEntries.get(partitionEntries.size() - 1).setMax(null);
		// 2.2 Create the partition map
		return new PartitionMap(PartitionMap.adjustEmptyPartitions(partitionEntries));
	}
	
	/**
	 * Create TupleMRBuilder for launching generation Job.
	 */
	protected TupleMRBuilder createMRBuilder(final int nPartitions, Configuration conf) throws TupleMRException, TupleSQLiteOutputFormatException {
		TupleMRBuilder builder = new TupleMRBuilder(conf, "Splout View Builder");

		List<TableSpec> tableSpecs = new ArrayList<TableSpec>();

		// For each Table we add an intermediate Pangool schema
		int schemaCounter = 0;

		for(Table table : tablespace.getPartitionedTables()) {
			List<Field> fields = new ArrayList<Field>();
			fields.addAll(table.getTableSpec().getSchema().getFields());
			fields.add(Field.create(TupleSQLite4JavaOutputFormat.PARTITION_TUPLE_FIELD, Type.INT));
			final Schema tableSchema = new Schema(table.getTableSpec().getSchema().getName(), fields);
			final TableSpec tableSpec = table.getTableSpec();
			schemaCounter++;
			builder.addIntermediateSchema(NullableSchema.nullableSchema(tableSchema));

			// For each input file for the Table we add an input and a TupleMapper
			for(TableInput inputFile : table.getFiles()) {

				final RecordProcessor recordProcessor = inputFile.getRecordProcessor();

				for(Path path : inputFile.getPaths()) {
					builder.addInput(path, inputFile.getFormat(), new TupleMapper<ITuple, NullWritable>() {

						Tuple tableTuple = new Tuple(tableSchema);
						JavascriptEngine jsEngine = null;
						CounterInterface counterInterface = null;

						@Override
						public void map(ITuple fileTuple, NullWritable value, TupleMRContext context,
						    Collector collector) throws IOException, InterruptedException {

							if(counterInterface == null) {
								counterInterface = new CounterInterface(context.getHadoopContext());
							}

							// Initialize JavaScript engine if needed
							if(jsEngine == null && tableSpec.getPartitionByJavaScript() != null) {
								try {
									jsEngine = new JavascriptEngine(tableSpec.getPartitionByJavaScript());
								} catch(Throwable e) {
									throw new RuntimeException(e);
								}
							}

							// For each input Tuple from this File execute the RecordProcessor
							// The Default IdentityRecordProcessor just bypasses the same Tuple
							ITuple processedTuple = null;
							try {
								processedTuple = recordProcessor.process(fileTuple, counterInterface);
							} catch(Throwable e1) {
								throw new RuntimeException(e1);
							}
							if(processedTuple == null) {
								// The tuple has been filtered out by the user
								return;
							}

							// Get the partition Id from this record
							String strKey = "";
							try {
								strKey = getPartitionByKey(processedTuple, tableSpec, jsEngine);
							} catch(Throwable e) {
								throw new RuntimeException(e);
							}
							int shardId = partitionMap.findPartition(strKey);
							if(shardId == -1) {
								throw new RuntimeException(
								    "shard id = -1 must be some sort of software bug. This shouldn't happen if PartitionMap is complete.");
							}

							// Finally write it to the Hadoop output
							for(Field field : processedTuple.getSchema().getFields()) {
								tableTuple.set(field.getName(), processedTuple.get(field.getName()));
							}
							tableTuple.set(TupleSQLite4JavaOutputFormat.PARTITION_TUPLE_FIELD, shardId);
							collector.write(tableTuple);
						}
					});
				}
			}
			tableSpecs.add(table.getTableSpec());
		}

		// We do the same for the replicated tables but the Mapper logic will be different
		// We will send the data to all the partitions
		for(final Table table : tablespace.getReplicateAllTables()) {
			List<Field> fields = new ArrayList<Field>();
			fields.addAll(table.getTableSpec().getSchema().getFields());
			fields.add(Field.create(TupleSQLite4JavaOutputFormat.PARTITION_TUPLE_FIELD, Type.INT));
			final Schema tableSchema = new Schema(table.getTableSpec().getSchema().getName(), fields);
			schemaCounter++;
			fields.add(Field.createTupleField("tuple", NullableSchema.nullableSchema(tableSchema)));
			builder.addIntermediateSchema(NullableSchema.nullableSchema(tableSchema));
			// For each input file for the Table we add an input and a TupleMapper
			for(TableInput inputFile : table.getFiles()) {

        final RecordProcessor recordProcessor = inputFile.getRecordProcessor();

				for(Path path : inputFile.getPaths()) {
					builder.addInput(path, inputFile.getFormat(), new TupleMapper<ITuple, NullWritable>() {

						Tuple tableTuple = new Tuple(tableSchema);
            CounterInterface counterInterface = null;

						@Override
						public void map(ITuple key, NullWritable value, TupleMRContext context, Collector collector)
						    throws IOException, InterruptedException {

              if(counterInterface == null) {
                counterInterface = new CounterInterface(context.getHadoopContext());
              }

              // For each input Tuple from this File execute the RecordProcessor
              // The Default IdentityRecordProcessor just bypasses the same Tuple
              ITuple processedTuple = null;
              try {
                processedTuple = recordProcessor.process(key, counterInterface);
              } catch(Throwable e1) {
                throw new RuntimeException(e1);
              }
              if(processedTuple == null) {
                // The tuple has been filtered out by the user
                return;
              }

              // Finally write it to the Hadoop output
              for(Field field : processedTuple.getSchema().getFields()) {
                tableTuple.set(field.getName(), processedTuple.get(field.getName()));
              }

							// Send the data of the replicated table to all partitions!
							for(int i = 0; i < nPartitions; i++) {
								tableTuple.set(TupleSQLite4JavaOutputFormat.PARTITION_TUPLE_FIELD, i);
								collector.write(tableTuple);
							}
						}
					});
				}
			}
			tableSpecs.add(table.getTableSpec());
		}

		// Group by partition
		builder.setGroupByFields(TupleSQLite4JavaOutputFormat.PARTITION_TUPLE_FIELD);

		if(schemaCounter == 1) {
			OrderBy orderBy = new OrderBy();
			orderBy.add(TupleSQLite4JavaOutputFormat.PARTITION_TUPLE_FIELD, Order.ASC);
			// The only table we have, check if it has specific order by
			OrderBy specificOrderBy = tablespace.getPartitionedTables().get(0).getTableSpec()
			    .getInsertionOrderBy();
			if(specificOrderBy != null) {
				for(SortElement elem : specificOrderBy.getElements()) {
					orderBy.add(elem.getName(), elem.getOrder());
				}
			}
			builder.setOrderBy(orderBy);
		} else { // > 1
			// More than one schema: set common order by
			builder.setOrderBy(OrderBy.parse(TupleSQLite4JavaOutputFormat.PARTITION_TUPLE_FIELD + ":asc")
			    .addSchemaOrder(Order.ASC));
			// And then as many particular order bys as needed - ....
			for(Table partitionedTable : tablespace.getPartitionedTables()) {
				if(partitionedTable.getTableSpec().getInsertionOrderBy() != null) {
					builder.setSpecificOrderBy(partitionedTable.getTableSpec().getSchema().getName(),
					    partitionedTable.getTableSpec().getInsertionOrderBy());
				}
			}
			for(Table replicatedTable : tablespace.getReplicateAllTables()) {
				if(replicatedTable.getTableSpec().getInsertionOrderBy() != null) {
					builder.setSpecificOrderBy(replicatedTable.getTableSpec().getSchema().getName(),
					    replicatedTable.getTableSpec().getInsertionOrderBy());
				}
			}
		}

		if(customReducer == null) {
			builder.setTupleReducer(new IdentityTupleReducer());
		} else {
			builder.setTupleReducer(customReducer);
		}

		builder.setJarByClass(TablespaceGenerator.class);
		// Define the output format - Tuple to SQL
		OutputFormat outputFormat = new TupleSQLite4JavaOutputFormat(batchSize,
		    tableSpecs.toArray(new TableSpec[0]));
		builder.setOutput(new Path(outputPath, OUT_STORE), outputFormat, ITuple.class, NullWritable.class);
		// #reducers = #partitions by default
		builder.getConf().setInt("mapred.reduce.tasks", nPartitions);

		return builder;
	}

	protected void executeViewGeneration(Job generationJob) throws IOException, InterruptedException,
	    ClassNotFoundException, TablespaceGeneratorException {

		long start = System.currentTimeMillis();
		generationJob.waitForCompletion(true);
		if(!generationJob.isSuccessful()) {
			throw new TablespaceGeneratorException("Error executing generation Job");
		}
		long end = System.currentTimeMillis();
		Log.info("Tablespace store generated in " + (end - start) + " ms.");
	}

	// Package-access, to be used for unit testing
	void setCustomReducer(TupleReducer<ITuple, NullWritable> customReducer) {
		this.customReducer = customReducer;
	}

	/**
	 * Returns the generated {@link PartitionMap}. It is also written to the HDFS. This is mainly used for testing.
	 */
	public PartitionMap getPartitionMap() {
		return partitionMap;
	}

	public int getRecordsToSample() {
		return recordsToSample;
	}

	public void setRecordsToSample(int recordsToSample) {
		this.recordsToSample = recordsToSample;
	}

	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}
}