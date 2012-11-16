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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.script.ScriptException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.mortbay.log.Log;

import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.tuplemr.Criteria.Order;
import com.datasalt.pangool.tuplemr.IdentityTupleReducer;
import com.datasalt.pangool.tuplemr.OrderBy;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.mapred.lib.input.TupleInputFormat.TupleInputReader;
import com.splout.db.common.JSONSerDe;
import com.splout.db.common.PartitionEntry;
import com.splout.db.common.PartitionMap;
import com.splout.db.common.Tablespace;

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
 * distributes the data accordingly. The Hadoop job will use {@link TupleSQLiteOutputFormat}.
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
	private transient final TablespaceSpec tablespace;

	// Number of keys that will be sampled for partitioning
	private int recordsToSample = 100000;
	// Number of SQL statements to execute before a COMMIT
	private int batchSize = 1000000;
	private PartitionMap partitionMap;

	public TablespaceGenerator(TablespaceSpec tablespace, Path outputPath) {
		this.tablespace = tablespace;
		this.outputPath = outputPath;
	}

	public final static String OUT_SAMPLED_INPUT = "sampled-input";
	public final static String OUT_PARTITION_MAP = "partition-map";
	public final static String OUT_INIT_STATEMENTS = "init-statements";
	public final static String OUT_STORE = "store";

	/**
	 * Return the partition key either by using partition-by-fields or partition-by-javascript as configured in the Table
	 * Spec.
	 * 
	 * @throws NoSuchMethodException
	 * @throws ScriptException
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

	public void generateView(Configuration conf, TupleSampler.SamplingType samplingType,
	    TupleSampler.SamplingOptions samplingOptions) throws Exception {
		FileSystem fileSystem = outputPath.getFileSystem(conf);
		fileSystem.mkdirs(outputPath);

		final int nPartitions = tablespace.getnPartitions();

		if(nPartitions > 1) {
			/*
			 * 1: Sample input
			 */
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
				RecordReader<ITuple, NullWritable> reader = new TupleInputReader(conf);
				((TupleInputReader) reader).initialize(sampledInput, conf);

				while(reader.nextKeyValue()) {
					ITuple tuple = new NullableTuple(reader.getCurrentKey());
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
			partitionMap = new PartitionMap(PartitionMap.adjustEmptyPartitions(partitionEntries));
		} else {
			partitionMap = PartitionMap.oneShardOpenedMap();
		}

		// 2.3 Write partition map to the HDFS
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

		/*
		 * 3: Generate view
		 */
		TupleMRBuilder builder = new TupleMRBuilder(conf, "Splout View Builder");

		List<TableSpec> tableSpecs = new ArrayList<TableSpec>();
		List<String> partitionedSchemaNames = new ArrayList<String>();

		// For each Table we add an intermediate Pangool schema
		int schemaCounter = 1;
		for(Table table : tablespace.getPartitionedTables()) {
			List<Field> fields = new ArrayList<Field>();
			// The schema is composed by the common field "partition" and the tuple meta field
			// The tuple meta field will be different for each Table but that is OK
			fields.add(Field.create("partition", Type.INT));
			fields.add(Field.create("key", Type.STRING));
			fields.add(Fields.createTupleField("tuple", new NullableSchema(table.getTableSpec().getSchema())));
			final Schema metaSchema = new Schema("metaSchema" + schemaCounter, fields);
			partitionedSchemaNames.add("metaSchema" + schemaCounter);
			final TableSpec tableSpec = table.getTableSpec();
			schemaCounter++;
			builder.addIntermediateSchema(metaSchema);
			final Schema schema = table.getTableSpec().getSchema();

			// For each input file for the Table we add an input and a TupleMapper
			for(TableInput inputFile : table.getFiles()) {

				final RecordProcessor recordProcessor = inputFile.getRecordProcessor();

				for(Path path : inputFile.getPaths()) {
					builder.addInput(path, inputFile.getFormat(), new TupleMapper<ITuple, NullWritable>() {

						ITuple metaTuple = new Tuple(metaSchema);
						NullableTuple tableTuple = new NullableTuple(schema);
						NullableTuple inputNullableTuple;
						NullableTupleView inputTupleView;
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

							// Wrap the input file Tuple into a NullableTuple for convenience
							if(inputNullableTuple == null) {
								inputNullableTuple = new NullableTuple(fileTuple);
								inputTupleView = new NullableTupleView(inputNullableTuple);
							} else {
								inputNullableTuple.setWrappedTuple(fileTuple);
							}

							// For each input Tuple from this File execute the RecordProcessor
							// The Default IdentityRecordProcessor just bypasses the same Tuple
							ITuple processedTuple = null;
							try {
								processedTuple = recordProcessor.process(inputTupleView, counterInterface);
							} catch(Throwable e1) {
								throw new RuntimeException(e1);
							}
							if(processedTuple == null) {
								// The tuple has been filtered out by the user
								return;
							}

							tableTuple.setWrappedTuple(processedTuple);

							// Get the partition Id from this record
							String strKey = "";
							try {
								strKey = getPartitionByKey(tableTuple, tableSpec, jsEngine);
							} catch(Throwable e) {
								throw new RuntimeException(e);
							}
							int shardId = partitionMap.findPartition(strKey);
							if(shardId == -1) {
								throw new RuntimeException(
								    "shard id = -1 must be some sort of software bug. This shouldn't happen if PartitionMap is complete.");
							}

							// Finally write it to the Hadoop output
							metaTuple.set("key", strKey);
							metaTuple.set("tuple", tableTuple);
							metaTuple.set("partition", shardId);
							collector.write(metaTuple);
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
			// The schema is composed by the common field "partition" and the tuple meta field
			// The tuple meta field will be different for each Table but that is OK
			fields.add(Field.create("partition", Type.INT));
			final Schema schema = table.getTableSpec().getSchema();
			fields.add(Fields.createTupleField("tuple", new NullableSchema(schema)));
			final Schema metaSchema = new Schema("metaSchema", fields);
			builder.addIntermediateSchema(metaSchema);
			// For each input file for the Table we add an input and a TupleMapper
			for(TableInput inputFile : table.getFiles()) {
				for(Path path : inputFile.getPaths()) {
					builder.addInput(path, inputFile.getFormat(), new TupleMapper<ITuple, NullWritable>() {

						ITuple metaTuple = new Tuple(metaSchema);
						NullableTuple tableTuple = new NullableTuple(schema);

						@Override
						public void map(ITuple key, NullWritable value, TupleMRContext context, Collector collector)
						    throws IOException, InterruptedException {

							tableTuple.setWrappedTuple(key);
							metaTuple.set("tuple", tableTuple);
							// Send the data of the replicated table to all partitions!
							for(int i = 0; i < nPartitions; i++) {
								metaTuple.set("partition", i);
								collector.write(metaTuple);
							}
						}
					});
				}
			}
			tableSpecs.add(table.getTableSpec());
		}

		// Group by partition
		builder.setGroupByFields("partition");
		if(tablespace.getReplicateAllTables().size() == 0) { // only partitioned tables
			// sort by partition and key
			builder.setOrderBy(OrderBy.parse("partition:desc, key:desc"));
		} else {
			// need to add specific ordering for partitioned tables only
			// replicated tables are not secondary sorted by key because they don't have a key
			builder.setOrderBy(OrderBy.parse("partition:desc").addSchemaOrder(Order.DESC));
			for(String partitionedSchema : partitionedSchemaNames) {
				builder.setSpecificOrderBy(partitionedSchema, OrderBy.parse("key:desc"));
			}
		}

		builder.setTupleReducer(new IdentityTupleReducer());
		
		builder.setJarByClass(TablespaceGenerator.class);
		// Define the output format - Tuple to SQL
		OutputFormat outputFormat = new TupleSQLite4JavaOutputFormat(batchSize,
		    tableSpecs.toArray(new TableSpec[0]));
		builder.setOutput(new Path(outputPath, OUT_STORE), outputFormat, ITuple.class, NullWritable.class);
		// #reducers = #partitions by default
		builder.getConf().setInt("mapred.reduce.tasks", nPartitions);
		long start = System.currentTimeMillis();
		Job job = builder.createJob();
		job.waitForCompletion(true);
		if(!job.isSuccessful()) {
			throw new TablespaceGeneratorException("Error executing generation Job");
		}
		long end = System.currentTimeMillis();
		Log.info("Tablespace store generated in " + (end - start) + " ms.");
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