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
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.tuplemr.Criteria.Order;
import com.datasalt.pangool.tuplemr.Criteria.SortElement;
import com.datasalt.pangool.tuplemr.*;
import com.splout.db.common.JSONSerDe;
import com.splout.db.common.JSONSerDe.JSONSerDeException;
import com.splout.db.common.PartitionEntry;
import com.splout.db.common.PartitionMap;
import com.splout.db.common.Tablespace;
import com.splout.db.engine.OutputFormatFactory;
import com.splout.db.hadoop.TupleSampler.TupleSamplerException;
import com.splout.db.hadoop.engine.SQLite4JavaOutputFormat;
import com.splout.db.hadoop.engine.SploutSQLOutputFormat;
import com.splout.db.hadoop.engine.SploutSQLOutputFormat.SploutSQLOutputFormatException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.mortbay.log.Log;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * A process that generates the SQL data stores needed for deploying a
 * tablespace in Splout, giving a file set table specification as input.
 * <p/>
 * The input to this process will be:
 * <ul>
 * <li>The {@link Tablespace} specification.</li>
 * <li>The Hadoop output Path</li>
 * </ul>
 * The output of the process is a Splout deployable path with a
 * {@link PartitionMap} . The format of the output is: outputPath + / +
 * {@link #OUT_PARTITION_MAP} for the partition map, outputPath + / +
 * {@link #OUT_SAMPLED_INPUT} for the list of sampled keys and outputPath + / +
 * {@link #OUT_STORE} for the folder containing the generated SQL store.
 * outputPath + / + {@link #OUT_ENGINE} is a file with the {@link SploutEngine}
 * id used to generate the tablespace.
 * <p/>
 * For creating the store we first sample the input dataset with
 * {@link TupleSampler} and then execute a Hadoop job that distributes the data
 * accordingly. The Hadoop job will use {@link SQLite4JavaOutputFormat}.
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
  protected transient final Path outputPath;
  protected transient TablespaceSpec tablespace;

  // Number of SQL statements to execute before a COMMIT
  private int batchSize = 1000000;
  protected PartitionMap partitionMap;

  private TupleReducer<ITuple, NullWritable> customReducer = null;

  // will be used to set the JarByClass
  protected Class callingClass;

  public TablespaceGenerator(TablespaceSpec tablespace, Path outputPath, Class callingClass) {
    this.tablespace = tablespace;
    this.outputPath = outputPath;
    this.callingClass = callingClass;
  }

  public final static String OUT_SAMPLED_INPUT = "sampled-input";
  public final static String OUT_SAMPLED_INPUT_SORTED = "sampled-input-sorted";
  public final static String OUT_PARTITION_MAP = "partition-map";
  public final static String OUT_INIT_STATEMENTS = "init-statements";
  public final static String OUT_STORE = "store";
  public final static String OUT_ENGINE = "engine";

  /**
   * Launches the generation of the tablespaces. Automatic
   * {@link com.splout.db.common.PartitionMap} calculation.
   * 
   * This is the public method which has to be called when using this class as
   * an API. Business logic has been split in various protected functions to
   * ease understading of it and also to be able to subclass this easily to
   * extend its functionality.
   */
  public void generateView(Configuration conf, TupleSampler.SamplingType samplingType,
      TupleSampler.SamplingOptions samplingOptions) throws Exception {

    prepareOutput(conf);

    final int nPartitions = tablespace.getnPartitions();

    if (nPartitions > 1) {
      partitionMap = sample(nPartitions, conf, samplingType, samplingOptions);
    } else {
      partitionMap = PartitionMap.oneShardOpenedMap();
    }

    Log.info("Calculated partition map: " + partitionMap);

    writeOutputMetadata(conf);

    TupleMRBuilder builder = createMRBuilder(nPartitions, conf);
    executeViewGeneration(builder);
  }

  /**
   * Launches the generation of the tablespaces. Uses the custom partition map
   * provided.
   * 
   * This is the public method which has to be called when using this class as
   * an API. Business logic has been split in various protected functions to
   * ease understading of it and also to be able to subclass this easily to
   * extend its functionality.
   */
  public void generateView(Configuration conf, PartitionMap partitionMap) throws Exception {

    prepareOutput(conf);

    this.partitionMap = partitionMap;
    final int nPartitions = partitionMap.getPartitionEntries().size();

    Log.info("Using provided partition map: " + partitionMap);

    writeOutputMetadata(conf);

    TupleMRBuilder builder = createMRBuilder(nPartitions, conf);
    executeViewGeneration(builder);
  }

  // ------------------------------- //

  protected void prepareOutput(Configuration conf) throws IOException {
    FileSystem fileSystem = outputPath.getFileSystem(conf);
    fileSystem.mkdirs(outputPath);
  }

  /**
   * Write the partition map and other metadata to the output folder. They will
   * be needed for deploying the dataset to Splout.
   */
  protected void writeOutputMetadata(Configuration conf) throws IOException, JSONSerDeException {
    FileSystem fileSystem = outputPath.getFileSystem(conf);

    // Write the Partition map
    Path partitionMapPath = new Path(outputPath, OUT_PARTITION_MAP);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fileSystem.create(partitionMapPath, true)));
    writer.write(JSONSerDe.ser(partitionMap));
    writer.close();

    // Write init statements, if applicable
    if (tablespace.getInitStatements() != null) {
      Path initStatementsPath = new Path(outputPath, OUT_INIT_STATEMENTS);
      writer = new BufferedWriter(new OutputStreamWriter(fileSystem.create(initStatementsPath, true)));
      writer.write(JSONSerDe.ser(tablespace.getInitStatements()));
      writer.close();
    }

    // Write the Engine ID so we know what we are deploying exactly afterwards
    Path enginePath = new Path(outputPath, OUT_ENGINE);
    writer = new BufferedWriter(new OutputStreamWriter(fileSystem.create(enginePath, true)));
    writer.write(tablespace.getEngine().getClass().getName());
    writer.close();
  }

  /**
   * Returns the partition key either by using partition-by-fields or
   * partition-by-javascript as configured in the Table Spec.
   */
  protected static String getPartitionByKey(ITuple tuple, TableSpec tableSpec, JavascriptEngine jsEngine)
      throws Throwable {

    String strKey = "";
    if (tableSpec.getPartitionFields() != null) {
      for (Field partitionField : tableSpec.getPartitionFields()) {
        Object obj = tuple.get(partitionField.getName());
        if (obj == null) {
          strKey += "";
        } else {
          strKey += obj.toString();
        }
      }
    } else {
      // use JavaScript
      strKey = jsEngine.execute("partition", tuple);
      if (strKey == null) {
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

    // Number of records to sample
    long recordsToSample = conf.getLong("splout.sampling.records.to.sample", 100000);

    // The sampler will generate a file with samples to use to create the
    // partition map
    Path sampledInput = new Path(outputPath, OUT_SAMPLED_INPUT);
    Path sampledInputSorted = new Path(outputPath, OUT_SAMPLED_INPUT_SORTED);
    TupleSampler sampler = new TupleSampler(samplingType, samplingOptions, callingClass);
    long retrivedSamples = sampler.sample(tablespace, conf, recordsToSample, sampledInput);

    // 1.1 Sorting sampled keys on disk
    fileSystem.delete(sampledInputSorted, true);
    SequenceFile.Sorter sorter = new SequenceFile.Sorter(fileSystem, Text.class, NullWritable.class, conf);
    sorter.sort(sampledInput, sampledInputSorted);

    // Start the reader
    @SuppressWarnings("deprecation")
    final SequenceFile.Reader reader = new SequenceFile.Reader(fileSystem, sampledInputSorted, conf);

    Log.info(retrivedSamples + " total keys sampled.");

    /*
     * 2: Calculate partition map
     */
    Nextable nextable = new Nextable() {
      @Override
      public boolean next(Writable writable) throws IOException {
        return reader.next(writable);
      }
    };
    List<PartitionEntry> partitionEntries = calculatePartitions(nPartitions, retrivedSamples, nextable);

    reader.close();
    fileSystem.delete(sampledInput, true);
    fileSystem.delete(sampledInputSorted, true);

    // 2.2 Create the partition map
    return new PartitionMap(partitionEntries);
  }

  // Useful for unit testing
  public static interface Nextable {
    public boolean next(Writable wriatable) throws IOException;
  }

  /**
   * Calculates the partitions given a sample. The following policy is followed:
   * <ul>
   * <li>Trying to create evenly sized partitions</li>
   * <li>No empty partitions allowed. Every partition must contain at least one
   * key</li>
   * <li>Number of retrieved partitions could be smaller than the requested
   * amount</li>
   * </ul>
   */
  static List<PartitionEntry> calculatePartitions(int nPartitions, long retrivedSamples, Nextable reader)
      throws IOException {
    // 2.1 Select "n" keys evenly distributed
    Text key = new Text();
    List<PartitionEntry> partitionEntries = new ArrayList<PartitionEntry>();
    int offset = Math.max(1, (int) (retrivedSamples / nPartitions));
    String min = null;
    int rowPointer = 0;
    boolean wereMore = true;
    String previousKey = null;
    String currentKey = null;
    String candidateToLastPartitionMin = null;
    boolean foundDistinctKey = false;
    for (int i = 1; i <= nPartitions; i++) {
      PartitionEntry entry = new PartitionEntry();
      if (min != null) {
        entry.setMin(min);
      }
      int keyIndex = i * offset;
      foundDistinctKey = false;
      do {
        wereMore = reader.next(key);
        if (wereMore) {
          rowPointer++;

          // Logic for knowing which is currentKey and previousKey
          if (!equalsWithNulls(key.toString(), currentKey)) {
            foundDistinctKey = true;
            previousKey = currentKey;
            currentKey = key.toString();
          }
        }
        // Keep iterating until we have advanced enough and we have find a
        // different key.
      } while (wereMore && (rowPointer < keyIndex || !foundDistinctKey));

      // If we are sure there are at least one partition more
      // we store the possible candidate to last partition min.
      if (wereMore && i < nPartitions) {
        candidateToLastPartitionMin = previousKey;
      }
      entry.setMax(key.toString());
      min = key.toString();
      entry.setShard(i - 1); // Shard are 0-indexed
      partitionEntries.add(entry);

      // No more rows to consume. No more partitions to build.
      if (!wereMore) {
        break;
      }
    }
    int generatedPartitions = partitionEntries.size();
    // Last range must be opened
    partitionEntries.get(generatedPartitions - 1).setMax(null);

    // Especial case. We want to ensure that every partition contains at least
    // one entry. Given than ranges are (,] that is ensured for every partition
    // but for the latest. We can ensure that the latest partition is not empty
    // if it has more sample keys after the latest min. That is, if
    // foundDistinctKey is true. Otherwise, we have to adjust:
    // We are going to try to adjust latest partition min
    // to a key before the selected min. If that is not possible, we merge
    // the latest to partitions. That solves the problem.
    if (!foundDistinctKey && partitionEntries.size() > 1) {
      PartitionEntry previous = partitionEntries.get(generatedPartitions - 2);
      PartitionEntry latest = partitionEntries.get(generatedPartitions - 1);
      // if previous.getMin() < candidateToLastPartitionMin
      // it is possible to adjust the latest two partitions
      if (compareWithNulls(previous.getMin(), candidateToLastPartitionMin) < 0) {
        previous.setMax(candidateToLastPartitionMin);
        latest.setMin(candidateToLastPartitionMin);
      } else {
        // Was not possible to adjust. Merging two latest partitions.
        previous.setMax(null);
        partitionEntries.remove(generatedPartitions - 1);
      }
    }
    return partitionEntries;
  }

  protected static int compareWithNulls(String a, String b) {
    if (a == null) {
      return (b == null ? 0 : -1);
    } else if (b == null) {
      return (a == null) ? 0 : 1;
    } else {
      return a.compareTo(b);
    }
  }

  /**
   * Create TupleMRBuilder for launching generation Job.
   */
  protected TupleMRBuilder createMRBuilder(final int nPartitions, Configuration conf) throws TupleMRException,
      SploutSQLOutputFormatException {
    TupleMRBuilder builder = new TupleMRBuilder(conf, "Splout generating " + outputPath);

    List<TableSpec> tableSpecs = new ArrayList<TableSpec>();

    // For each Table we add an intermediate Pangool schema
    int schemaCounter = 0;

    for (Table table : tablespace.getPartitionedTables()) {
      List<Field> fields = new ArrayList<Field>();
      fields.addAll(table.getTableSpec().getSchema().getFields());
      fields.add(SploutSQLOutputFormat.getPartitionField());
      final Schema tableSchema = new Schema(table.getTableSpec().getSchema().getName(), fields);
      final TableSpec tableSpec = table.getTableSpec();
      schemaCounter++;
      builder.addIntermediateSchema(NullableSchema.nullableSchema(tableSchema));

      // For each input file for the Table we add an input and a TupleMapper
      for (TableInput inputFile : table.getFiles()) {

        final RecordProcessor recordProcessor = inputFile.getRecordProcessor();

        for (Path path : inputFile.getPaths()) {
          builder.addInput(path, inputFile.getFormat(), new TupleMapper<ITuple, NullWritable>() {

            Tuple tableTuple = new Tuple(tableSchema);
            JavascriptEngine jsEngine = null;
            CounterInterface counterInterface = null;

            @Override
            public void map(ITuple fileTuple, NullWritable value, TupleMRContext context, Collector collector)
                throws IOException, InterruptedException {

              if (counterInterface == null) {
                counterInterface = new CounterInterface(context.getHadoopContext());
              }

              // Initialize JavaScript engine if needed
              if (jsEngine == null && tableSpec.getPartitionByJavaScript() != null) {
                try {
                  jsEngine = new JavascriptEngine(tableSpec.getPartitionByJavaScript());
                } catch (Throwable e) {
                  throw new RuntimeException(e);
                }
              }

              // For each input Tuple from this File execute the RecordProcessor
              // The Default IdentityRecordProcessor just bypasses the same
              // Tuple
              ITuple processedTuple = null;
              try {
                processedTuple = recordProcessor.process(fileTuple, counterInterface);
              } catch (Throwable e1) {
                throw new RuntimeException(e1);
              }
              if (processedTuple == null) {
                // The tuple has been filtered out by the user
                return;
              }

              // Get the partition Id from this record
              String strKey = "";
              try {
                strKey = getPartitionByKey(processedTuple, tableSpec, jsEngine);
              } catch (Throwable e) {
                throw new RuntimeException(e);
              }

              // The record processor may have a special way of determining the
              // partition for this tuple
              int shardId = recordProcessor.getPartition(strKey, processedTuple, counterInterface);
              if (shardId == PartitionMap.NO_PARTITION) {
                shardId = partitionMap.findPartition(strKey);
                if (shardId == PartitionMap.NO_PARTITION) {
                  throw new RuntimeException(
                      "shard id = -1 must be some sort of software bug. This shouldn't happen if PartitionMap is complete.");
                }
              }

              // Finally write it to the Hadoop output
              for (Field field : processedTuple.getSchema().getFields()) {
                tableTuple.set(field.getName(), processedTuple.get(field.getName()));
              }
              tableTuple.set(SploutSQLOutputFormat.PARTITION_TUPLE_FIELD, shardId);
              collector.write(tableTuple);
            }
          }, inputFile.getSpecificHadoopInputFormatContext());
        }
      }
      tableSpecs.add(table.getTableSpec());
    }

    // We do the same for the replicated tables but the Mapper logic will be
    // different
    // We will send the data to all the partitions
    for (final Table table : tablespace.getReplicateAllTables()) {
      List<Field> fields = new ArrayList<Field>();
      fields.addAll(table.getTableSpec().getSchema().getFields());
      fields.add(SploutSQLOutputFormat.getPartitionField());
      final Schema tableSchema = new Schema(table.getTableSpec().getSchema().getName(), fields);
      schemaCounter++;
      builder.addIntermediateSchema(NullableSchema.nullableSchema(tableSchema));
      // For each input file for the Table we add an input and a TupleMapper
      for (TableInput inputFile : table.getFiles()) {

        final RecordProcessor recordProcessor = inputFile.getRecordProcessor();

        for (Path path : inputFile.getPaths()) {
          builder.addInput(path, inputFile.getFormat(), new TupleMapper<ITuple, NullWritable>() {

            Tuple tableTuple = new Tuple(tableSchema);
            CounterInterface counterInterface = null;

            @Override
            public void map(ITuple key, NullWritable value, TupleMRContext context, Collector collector)
                throws IOException, InterruptedException {

              if (counterInterface == null) {
                counterInterface = new CounterInterface(context.getHadoopContext());
              }

              // For each input Tuple from this File execute the RecordProcessor
              // The Default IdentityRecordProcessor just bypasses the same
              // Tuple
              ITuple processedTuple = null;
              try {
                processedTuple = recordProcessor.process(key, counterInterface);
              } catch (Throwable e1) {
                throw new RuntimeException(e1);
              }
              if (processedTuple == null) {
                // The tuple has been filtered out by the user
                return;
              }

              // Finally write it to the Hadoop output
              for (Field field : processedTuple.getSchema().getFields()) {
                tableTuple.set(field.getName(), processedTuple.get(field.getName()));
              }

              // Send the data of the replicated table to all partitions!
              for (int i = 0; i < nPartitions; i++) {
                tableTuple.set(SploutSQLOutputFormat.PARTITION_TUPLE_FIELD, i);
                collector.write(tableTuple);
              }
            }
          }, inputFile.getSpecificHadoopInputFormatContext());
        }
      }
      tableSpecs.add(table.getTableSpec());
    }

    // Group by partition
    builder.setGroupByFields(SploutSQLOutputFormat.PARTITION_TUPLE_FIELD);

    if (schemaCounter == 1) {
      OrderBy orderBy = new OrderBy();
      orderBy.add(SploutSQLOutputFormat.PARTITION_TUPLE_FIELD, Order.ASC);
      // The only table we have, check if it has specific order by
      OrderBy specificOrderBy = tablespace.getPartitionedTables().get(0).getTableSpec().getInsertionOrderBy();
      if (specificOrderBy != null) {
        for (SortElement elem : specificOrderBy.getElements()) {
          orderBy.add(elem.getName(), elem.getOrder());
        }
      }
      builder.setOrderBy(orderBy);
    } else { // > 1
      // More than one schema: set common order by
      builder.setOrderBy(OrderBy.parse(SploutSQLOutputFormat.PARTITION_TUPLE_FIELD + ":asc").addSchemaOrder(Order.ASC));
      // And then as many particular order bys as needed - ....
      for (Table partitionedTable : tablespace.getPartitionedTables()) {
        if (partitionedTable.getTableSpec().getInsertionOrderBy() != null) {
          builder.setSpecificOrderBy(partitionedTable.getTableSpec().getSchema().getName(), partitionedTable
              .getTableSpec().getInsertionOrderBy());
        }
      }
      for (Table replicatedTable : tablespace.getReplicateAllTables()) {
        if (replicatedTable.getTableSpec().getInsertionOrderBy() != null) {
          builder.setSpecificOrderBy(replicatedTable.getTableSpec().getSchema().getName(), replicatedTable
              .getTableSpec().getInsertionOrderBy());
        }
      }
    }

    if (customReducer == null) {
      builder.setTupleReducer(new IdentityTupleReducer());
    } else {
      builder.setTupleReducer(customReducer);
    }

    builder.setJarByClass(callingClass);
    // Define the output format

    TableSpec[] tbls = tableSpecs.toArray(new TableSpec[0]);
    OutputFormat outputFormat = null;
    try {
      outputFormat = OutputFormatFactory.getOutputFormat(tablespace.getEngine(), batchSize, tbls);
    } catch (Exception e) {
      System.err.println(e);
      throw new RuntimeException(e);
    }

    builder.setOutput(new Path(outputPath, OUT_STORE), outputFormat, ITuple.class, NullWritable.class);
    // #reducers = #partitions by default
    builder.getConf().setInt("mapred.reduce.tasks", nPartitions);

    return builder;
  }

  protected void executeViewGeneration(TupleMRBuilder builder) throws IOException, InterruptedException,
      ClassNotFoundException, TablespaceGeneratorException, TupleMRException {

    try {
      Job generationJob = builder.createJob();
      long start = System.currentTimeMillis();
      generationJob.waitForCompletion(true);
      if (!generationJob.isSuccessful()) {
        throw new TablespaceGeneratorException("Error executing generation Job");
      }
      long end = System.currentTimeMillis();
      Log.info("Tablespace store generated in " + (end - start) + " ms.");
    } finally {
      builder.cleanUpInstanceFiles();
    }
  }

  // Package-access, to be used for unit testing
  void setCustomReducer(TupleReducer<ITuple, NullWritable> customReducer) {
    this.customReducer = customReducer;
  }

  /**
   * Returns the generated {@link PartitionMap}. It is also written to the HDFS.
   * This is mainly used for testing.
   */
  public PartitionMap getPartitionMap() {
    return partitionMap;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  protected static final boolean equalsWithNulls(Object a, Object b) {
    if (a == b)
      return true;
    if ((a == null) || (b == null))
      return false;
    return a.equals(b);
  }
}
