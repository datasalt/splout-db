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

import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.tuplemr.Criteria.SortElement;
import com.datasalt.pangool.tuplemr.OrderBy;
import com.datasalt.pangool.tuplemr.mapred.lib.input.TupleInputFormat;
import com.datasalt.pangool.tuplemr.mapred.lib.input.TupleTextInputFormat;
import com.splout.db.hadoop.TableSpec.FieldIndex;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;

import java.util.*;

/**
 * This builder can be used to obtain {@link Table} beans. These beans can then be used to obtain a
 * {@link TablespaceSpec} through {@link TablespaceBuilder}.
 */
public class TableBuilder {

	private final static Log log = LogFactory.getLog(TableBuilder.class);

  /**
   * Exception that is thrown if a Table cannot be built because there is missing data or inconsistent data has been specified. The reason is specified as the message of the Exception.
	 */
	@SuppressWarnings("serial")
	public static class TableBuilderException extends Exception {

		public TableBuilderException(String msg) {
			super(msg);
		}
	}

	private final Schema schema;
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

	public TableBuilder(final Schema schema) {
		this.schema = schema;
	}

	public TableBuilder addFixedWidthTextFile(Path path, Schema schema, int []fields, boolean hasHeader, String nullString, RecordProcessor recordProcessor) {
		addFile(new TableInput(new TupleTextInputFormat(schema, fields, hasHeader, nullString), schema, (recordProcessor == null) ? new IdentityRecordProcessor() : recordProcessor, path));
		return this;
	}

	public TableBuilder addCSVTextFile(Path path, char separator, char quoteCharacter, char escapeCharacter, boolean hasHeader, boolean strictQuotes, String nullString, Schema fileSchema, RecordProcessor recordProcessor) {
		addFile(new TableInput(new TupleTextInputFormat(fileSchema, hasHeader, strictQuotes, separator, quoteCharacter, escapeCharacter, TupleTextInputFormat.FieldSelector.NONE, nullString), fileSchema, recordProcessor, path));
		return this;
	}

	public TableBuilder addCSVTextFile(String path, char separator, char quoteCharacter, char escapeCharacter, boolean hasHeader, boolean strictQuotes, String nullString, Schema fileSchema, RecordProcessor recordProcessor) {
		return addCSVTextFile(new Path(path), separator, quoteCharacter, escapeCharacter, hasHeader, strictQuotes, nullString, fileSchema, recordProcessor);
	}

	public TableBuilder addCSVTextFile(Path path, char separator, char quoteCharacter, char escapeCharacter, boolean hasHeader, boolean strictQuotes, String nullString) {
		return addCSVTextFile(path, separator, quoteCharacter, escapeCharacter, hasHeader, strictQuotes, nullString, schema, new IdentityRecordProcessor());
	}

	public TableBuilder addCSVTextFile(String path, char separator, char quoteCharacter, char escapeCharacter, boolean hasHeader, boolean strictQuotes, String nullString) {
		return addCSVTextFile(path, separator, quoteCharacter, escapeCharacter, hasHeader, strictQuotes, nullString, schema, new IdentityRecordProcessor());
	}

	public TableBuilder addCSVTextFile(Path path, Schema fileSchema, RecordProcessor recordProcessor) {
		return addCSVTextFile(path, '\t', TupleTextInputFormat.NO_QUOTE_CHARACTER, TupleTextInputFormat.NO_ESCAPE_CHARACTER, false, false, TupleTextInputFormat.NO_NULL_STRING, fileSchema, recordProcessor);
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

	public TableBuilder addTupleFile(Path path) {
		addTupleFile(path, null);
		return this;
	}

  public TableBuilder addTupleFile(Path path, RecordProcessor recordProcessor) {
    addFile(new TableInput(new TupleInputFormat(), schema, (recordProcessor == null) ? new IdentityRecordProcessor() : recordProcessor, path));
    return this;
  }


  /**
   * @param initialSQLStatements SQL statements that will be executed at the start of the process, just after
   *                             some default PRAGMA statements and just before the CREATE TABLE statements.
   */
  public TableBuilder initialSQL(String... initialSQLStatements) {
    this.initialSQL = initialSQLStatements;
    return this;
  }

  /**
   * @param preInsertsSQLStatements SQL statements that will be executed just after the CREATE TABLE statements
   *                      but just before the INSERT statements used to insert data.
   */
  public TableBuilder preInsertsSQL(String... preInsertsSQLStatements) {
    this.preInsertsSQL = preInsertsSQLStatements;
    return this;
  }

  /**
   * @param postInsertsSQLStatements SQL statements that will be executed just after all data is inserted but
   *                                 just before the CREATE INDEX statements.
   */
  public TableBuilder postInsertsSQL(String... postInsertsSQLStatements) {
    this.postInsertsSQL = postInsertsSQLStatements;
    return this;
  }

  /**
   * @param finalSQLStatements SQL statements that will be executed al the end of the process, just after the
   *                           CREATE INDEX statements.
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
	    throw new TableBuilderException("Invalid javascript, must contain partition() function that receives a record: " + e.getMessage());
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
		for(SortElement element: orderBy.getElements()) {
			if(!schema.containsField(element.getName())) {
				throw new TableBuilderException("Order by field: " + element.getName() + " not contained in table schema.");
			}
		}
		this.orderBy = orderBy;
		return this;
	}

	public Table build() throws TableBuilderException {
		if(schema == null) {
			throw new TableBuilderException("No schema for table: Can't build a Table without a Schema.");
		}

		Field[] partitionBySchemaFields = null;
		if(!isReplicated) { // Check that partition field is good
			// Check that is present in schema
			if(partitionByFields == null && partitionByJavaScript == null) {
				throw new TableBuilderException("No partition fields or partition-by-JavaScript for a non replicated table. Must specify at least one.");
			}
			if(partitionByFields != null) {
				partitionBySchemaFields = new Field[partitionByFields.length];
				int i = 0;
				for(String partitionByField: partitionByFields) {
					Field partitionField = schema.getField(partitionByField);
					if(partitionField == null) {
						throw new TableBuilderException("Invalid partition field: " + partitionByField + " not present in its Schema: "
						    + schema + ".");
					}
					partitionBySchemaFields[i] = partitionField;
					i++;
				}
			}
		} else {
			if(partitionByFields != null) {
				throw new TableBuilderException("Replicated table with partition fields is an inconsistent specification. Please check if you are doing something wrong.");
			}
		}

		// Indexes
		List<FieldIndex> indexes = new ArrayList<FieldIndex>();
		for(String fieldToIndex : fieldsToIndex) {
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
				// Check that each field exists in schema
				Field field2 = schema.getField(field);
				if(field2 == null) {
					throw new TableBuilderException("Invalid compound index: " + compoundIndex + ", field " + field2
					    + " not present in specified Schema: " + schema + ".");
				}
				compoundIndexFields.add(field2);
			}
			indexes.add(new FieldIndex(compoundIndexFields.toArray(new Field[0])));
		}

		// Schema + indexes = TableSpec
		TableSpec spec;
		FieldIndex[] theIndexes = indexes.toArray(new FieldIndex[0]);

		if(partitionByJavaScript != null) {
      spec = new TableSpec(schema, partitionByJavaScript, theIndexes, initialSQL, preInsertsSQL, postInsertsSQL, finalSQL, orderBy);
    } else {
      spec = new TableSpec(schema, partitionBySchemaFields, theIndexes, initialSQL, preInsertsSQL, postInsertsSQL, finalSQL, orderBy);
		}

		// Now get the input Paths
		if(files == null || files.size() == 0) {
			throw new TableBuilderException("No files added, must add at least one.");
		}

		// Final immutable Table bean
		return new Table(files, spec);
	}
}
