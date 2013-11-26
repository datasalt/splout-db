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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.tuplemr.Criteria.SortElement;
import com.datasalt.pangool.tuplemr.OrderBy;
import com.datasalt.pangool.tuplemr.mapred.lib.input.CascadingTupleInputFormat;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HCatTupleInputFormat;
import com.datasalt.pangool.tuplemr.mapred.lib.input.TupleInputFormat;
import com.datasalt.pangool.tuplemr.mapred.lib.input.TupleTextInputFormat;
import com.splout.db.hadoop.TableSpec.FieldIndex;

/**
 * This builder can be used to obtain {@link Table} beans. These beans can then be used to obtain a
 * {@link TablespaceSpec} through {@link TablespaceBuilder}.
 */
public class TableBuilder {

	private final static Log log = LogFactory.getLog(TableBuilder.class);

	/**
	 * Exception that is thrown if a Table cannot be built because there is missing data or inconsistent data has been
	 * specified. The reason is specified as the message of the Exception.
	 */
	@SuppressWarnings("serial")
	public static class TableBuilderException extends Exception {

		public TableBuilderException(String msg) {
			super(msg);
		}
	}

	private Schema schema;
	private List<TableInput> files = new ArrayList<TableInput>();
	private String[] partitionByFields;
	private String partitionByJavaScript = null;
	private boolean isReplicated = false;
	private Set<String> fieldsToIndex = new HashSet<String>();
	private List<List<String>> compoundIndexes = new ArrayList<List<String>>();
	private String[] initialSQL = null;
	private String[] preInsertsSQL = null;
	private String[] postInsertsSQL;
	private String[] finalSQL = null;
	private OrderBy orderBy;
	private String tableName = null;

	// the Hadoop conf can be provided as an alternative to the Schema to be able to sample it from the input files in
	// case the input files are not Textual
	private Configuration hadoopConf;

	/**
	 * Fixed schema constructor: for example, if we use textual files. The table name is extracted from the Schema name.
	 */
	public TableBuilder(final Schema schema) {
		this(null, schema);
	}

	/**
	 * Fixed schema + explicit table name.
	 */
	public TableBuilder(final String tableName, final Schema schema) {
		if(schema == null) {
			throw new IllegalArgumentException(
			    "Explicit table schema can't be null - please use the other constructors for implicit Schema discovering.");
		}
		this.tableName = tableName;
		this.schema = schema;
	}

	/**
	 * Hadoop configuration, no schema: The input files will contain the Schema (e.g. Tuple files / Cascading files).
	 */
	public TableBuilder(final Configuration hadoopConf) {
		this(null, hadoopConf);
	}

	/**
	 * Schema-less constructor with explicit table name.
	 */
	public TableBuilder(final String tableName, final Configuration hadoopConf) {
		if(hadoopConf == null) {
			throw new IllegalArgumentException(
			    "Hadoop configuration can't be null - please provide a valid one.");
		}
		this.tableName = tableName;
		this.hadoopConf = hadoopConf;
	}

	public TableBuilder addFixedWidthTextFile(Path path, Schema schema, int[] fields, boolean hasHeader,
	    String nullString, RecordProcessor recordProcessor) {
		addFile(new TableInput(new TupleTextInputFormat(schema, fields, hasHeader, nullString),
		    new HashMap<String, String>(), schema, (recordProcessor == null) ? new IdentityRecordProcessor()
		        : recordProcessor, path));
		return this;
	}

	public TableBuilder addCSVTextFile(Path path, char separator, char quoteCharacter,
	    char escapeCharacter, boolean hasHeader, boolean strictQuotes, String nullString,
	    Schema fileSchema, RecordProcessor recordProcessor) {
		return addFile(new TableInput(
		    new TupleTextInputFormat(fileSchema, hasHeader, strictQuotes, separator, quoteCharacter,
		        escapeCharacter, TupleTextInputFormat.FieldSelector.NONE, nullString),
		    new HashMap<String, String>(), fileSchema, recordProcessor, path));
	}

	public TableBuilder addCSVTextFile(String path, char separator, char quoteCharacter,
	    char escapeCharacter, boolean hasHeader, boolean strictQuotes, String nullString,
	    Schema fileSchema, RecordProcessor recordProcessor) {
		return addCSVTextFile(new Path(path), separator, quoteCharacter, escapeCharacter, hasHeader,
		    strictQuotes, nullString, fileSchema, recordProcessor);
	}

	public TableBuilder addCSVTextFile(Path path, char separator, char quoteCharacter,
	    char escapeCharacter, boolean hasHeader, boolean strictQuotes, String nullString) {
		return addCSVTextFile(path, separator, quoteCharacter, escapeCharacter, hasHeader, strictQuotes,
		    nullString, schema, new IdentityRecordProcessor());
	}

	public TableBuilder addCSVTextFile(String path, char separator, char quoteCharacter,
	    char escapeCharacter, boolean hasHeader, boolean strictQuotes, String nullString) {
		return addCSVTextFile(path, separator, quoteCharacter, escapeCharacter, hasHeader, strictQuotes,
		    nullString, schema, new IdentityRecordProcessor());
	}

	public TableBuilder addCSVTextFile(Path path, Schema fileSchema, RecordProcessor recordProcessor) {
		return addCSVTextFile(path, '\t', TupleTextInputFormat.NO_QUOTE_CHARACTER,
		    TupleTextInputFormat.NO_ESCAPE_CHARACTER, false, false, TupleTextInputFormat.NO_NULL_STRING,
		    fileSchema, recordProcessor);
	}

	public TableBuilder addCSVTextFile(String path, Schema fileSchema, RecordProcessor recordProcessor) {
		return addCSVTextFile(new Path(path), fileSchema, recordProcessor);
	}

	public TableBuilder addCSVTextFile(Path path) {
		return addCSVTextFile(path, schema, new IdentityRecordProcessor());
	}

	public TableBuilder addCSVTextFile(String path) {
		return addCSVTextFile(path, schema, new IdentityRecordProcessor());
	}

	public TableBuilder addHiveTable(String dbName, String tableName) throws IOException {
		if(hadoopConf == null) {
			throw new IllegalArgumentException(
			    "Can't use this method if the builder hasn't been instantiated with a Hadoop conf. object!");
		}
		return addHiveTable(dbName, tableName, hadoopConf);
	}

	public TableBuilder addHiveTable(String dbName, String tableName, Configuration conf)
	    throws IOException {
		if(hadoopConf == null) {
			throw new IllegalArgumentException(
			    "Hadoop configuration can't be null - please provide a valid one.");
		}
		HCatTupleInputFormat inputFormat = new HCatTupleInputFormat(dbName, tableName, conf);
		Map<String, String> specificContext = new HashMap<String, String>();
		specificContext.put("mapreduce.lib.hcat.job.info", conf.get("mapreduce.lib.hcat.job.info"));
		specificContext.put("mapreduce.lib.hcatoutput.hive.conf",
		    conf.get("mapreduce.lib.hcatoutput.hive.conf"));
		addCustomInputFormatFile(new Path("hive/" + dbName + "/" + this.tableName), inputFormat,
		    specificContext, new IdentityRecordProcessor());
		return this;
	}

	public TableBuilder addCascadingTable(Path path, String[] columnNames) throws IOException {
		if(hadoopConf == null) {
			throw new IllegalArgumentException(
			    "Can't use this method if the builder hasn't been instantiated with a Hadoop conf. object!");
		}
		return addCascadingTable(path, columnNames, hadoopConf);
	}

	public TableBuilder addCascadingTable(Path inputPath, String[] columnNames, Configuration conf)
	    throws IOException {
		CascadingTupleInputFormat.setSerializations(conf);
		if(tableName == null) {
			throw new IllegalArgumentException(
			    "A table name should have been provided by constructor for using this method.");
		}
		return addCustomInputFormatFile(inputPath, new CascadingTupleInputFormat(tableName, columnNames));
	}

	public TableBuilder addCustomInputFormatFile(Path path, InputFormat<ITuple, NullWritable> inputFormat)
	    throws IOException {
		return addCustomInputFormatFile(path, inputFormat, null);
	}

	public TableBuilder addCustomInputFormatFile(Path path, InputFormat<ITuple, NullWritable> inputFormat,
	    RecordProcessor recordProcessor) throws IOException {
		return addCustomInputFormatFile(path, inputFormat, new HashMap<String, String>(), recordProcessor);
	}

	public TableBuilder addCustomInputFormatFile(Path path, InputFormat<ITuple, NullWritable> inputFormat,
	    Map<String, String> specificContext, RecordProcessor recordProcessor) throws IOException {
		if(schema == null) {
			// sample it
			try {
				schema = SchemaSampler.sample(hadoopConf, path, inputFormat);
			} catch(InterruptedException e) {
				throw new IOException(e);
			}
		}
		return addFile(new TableInput(inputFormat, specificContext, schema,
		    (recordProcessor == null) ? new IdentityRecordProcessor() : recordProcessor, path));
	}

	public TableBuilder addTupleFile(Path path) throws IOException {
		return addTupleFile(path, (RecordProcessor)null);
	}

	public TableBuilder addTupleFile(Path path, Schema explicitSchema) throws IOException {
		return addTupleFile(path, explicitSchema, null);
	}
	
	public TableBuilder addTupleFile(Path path, RecordProcessor recordProcessor) throws IOException {
		return addCustomInputFormatFile(path, new TupleInputFormat(), recordProcessor);
	}

	public TableBuilder addTupleFile(Path path, Schema explicitSchema, RecordProcessor recordProcessor) throws IOException {
		return addCustomInputFormatFile(path, new TupleInputFormat(explicitSchema), recordProcessor);
	}

	/**
	 * @param initialSQLStatements
	 *          SQL statements that will be executed at the start of the process, just after some default PRAGMA
	 *          statements and just before the CREATE TABLE statements.
	 */
	public TableBuilder initialSQL(String... initialSQLStatements) {
		this.initialSQL = initialSQLStatements;
		return this;
	}

	/**
	 * @param preInsertsSQLStatements
	 *          SQL statements that will be executed just after the CREATE TABLE statements but just before the INSERT
	 *          statements used to insert data.
	 */
	public TableBuilder preInsertsSQL(String... preInsertsSQLStatements) {
		this.preInsertsSQL = preInsertsSQLStatements;
		return this;
	}

	/**
	 * @param postInsertsSQLStatements
	 *          SQL statements that will be executed just after all data is inserted but just before the CREATE INDEX
	 *          statements.
	 */
	public TableBuilder postInsertsSQL(String... postInsertsSQLStatements) {
		this.postInsertsSQL = postInsertsSQLStatements;
		return this;
	}

	/**
	 * @param finalSQLStatements
	 *          SQL statements that will be executed al the end of the process, just after the CREATE INDEX statements.
	 */
	public TableBuilder finalSQL(String... finalSQLStatements) {
		this.finalSQL = finalSQLStatements;
		return this;
	}

	public TableBuilder createIndex(String... indexFields) {
		if(indexFields.length == 1) {
			fieldsToIndex.add(indexFields[0]);
		} else {
			compoundIndexes.add(Arrays.asList(indexFields));
		}
		return this;
	}

	public TableBuilder partitionBy(String... partitionByFields) {
		this.partitionByFields = partitionByFields;
		return this;
	}

	public TableBuilder partitionByJavaScript(String javascript) throws TableBuilderException {
		// Check that javascript is valid
		// It must be evaluated and we will also check that it has a "partition" function
		JavascriptEngine engine;
		try {
			engine = new JavascriptEngine(javascript);
		} catch(Throwable e) {
			log.error("Error evaluating javascript", e);
			throw new TableBuilderException("Invalid javascript: " + e.getMessage());
		}

		try {
			engine.execute("partition", new Object[0]);
		} catch(ClassCastException e) {
			log.error("Error evaluating javascript, doesn't contain partition function", e);
			throw new TableBuilderException(
			    "Invalid javascript, must contain partition() function that receives a record: "
			        + e.getMessage());
		} catch(Throwable e) {
			// skip - might be null pointers as we are passing a null object
		}

		partitionByJavaScript = javascript;
		return this;
	}

	public TableBuilder replicateToAll() {
		isReplicated = true;
		return this;
	}

	public TableBuilder addFile(TableInput tableFile) {
		files.add(tableFile);
		return this;
	}

	public TableBuilder insertionSortOrder(OrderBy orderBy) throws TableBuilderException {
		for(SortElement element : orderBy.getElements()) {
			if(!schema.containsField(element.getName())) {
				throw new TableBuilderException("Order by field: " + element.getName()
				    + " not contained in table schema.");
			}
		}
		this.orderBy = orderBy;
		return this;
	}

	public Table build() throws TableBuilderException {
		if(schema == null) {
			throw new TableBuilderException("No schema for table: Can't build a Table without a Schema.");
		}

		if(tableName != null) {
			// Schema name and table name may actually differ:
			// This might happen when sampling the schema from some custom InputFormat.
			// E.g. a Hive table - we might want to rename it here for importing it more than once.
			this.schema = new Schema(tableName, schema.getFields());
		}

		Field[] partitionBySchemaFields = null;
		if(!isReplicated) { // Check that partition field is good
			// Check that is present in schema
			if(partitionByFields == null && partitionByJavaScript == null) {
				throw new TableBuilderException(
				    "No partition fields or partition-by-JavaScript for a non replicated table. Must specify at least one.");
			}
			if(partitionByFields != null) {
				partitionBySchemaFields = new Field[partitionByFields.length];
				int i = 0;
				for(String partitionByField : partitionByFields) {
					partitionByField = partitionByField.trim();
					Field partitionField = schema.getField(partitionByField);
					if(partitionField == null) {
						throw new TableBuilderException("Invalid partition field: " + partitionByField
						    + " not present in its Schema: " + schema + ".");
					}
					partitionBySchemaFields[i] = partitionField;
					i++;
				}
			}
		} else {
			if(partitionByFields != null) {
				throw new TableBuilderException(
				    "Replicated table with partition fields is an inconsistent specification. Please check if you are doing something wrong.");
			}
		}

		// Indexes
		List<FieldIndex> indexes = new ArrayList<FieldIndex>();
		for(String fieldToIndex : fieldsToIndex) {
			fieldToIndex = fieldToIndex.trim();
			// Check that field exists in schema
			Field field1 = schema.getField(fieldToIndex);
			if(field1 == null) {
				throw new TableBuilderException("Invalid field to index: " + fieldToIndex
				    + " not present in specified Schema: " + schema + ".");
			}
			indexes.add(new FieldIndex(field1));
		}
		// Also, support for compound indexes
		for(List<String> compoundIndex : compoundIndexes) {
			List<Field> compoundIndexFields = new ArrayList<Field>();
			for(String field : compoundIndex) {
				field = field.trim();
				// Check that each field exists in schema
				Field field2 = schema.getField(field);
				if(field2 == null) {
					throw new TableBuilderException("Invalid compound index: " + compoundIndex + ", field "
					    + field + " not present in specified Schema: " + schema + ".");
				}
				compoundIndexFields.add(field2);
			}
			indexes.add(new FieldIndex(compoundIndexFields.toArray(new Field[0])));
		}

		// Schema + indexes = TableSpec
		TableSpec spec;
		FieldIndex[] theIndexes = indexes.toArray(new FieldIndex[0]);

		if(partitionByJavaScript != null) {
			spec = new TableSpec(schema, partitionByJavaScript, theIndexes, initialSQL, preInsertsSQL,
			    postInsertsSQL, finalSQL, orderBy);
		} else {
			spec = new TableSpec(schema, partitionBySchemaFields, theIndexes, initialSQL, preInsertsSQL,
			    postInsertsSQL, finalSQL, orderBy);
		}

		// Now get the input Paths
		if(files == null || files.size() == 0) {
			throw new TableBuilderException("No files added, must add at least one.");
		}

		// Final immutable Table bean
		return new Table(files, spec);
	}
}
