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
import com.datasalt.pangool.tuplemr.OrderBy;
import com.splout.db.hadoop.engine.SQLite4JavaOutputFormat;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Simple immutable bean that specifies the Pangool Schema of a Splout Table and the Fields that need to be indexed and how it is partitioned.
 * It is part of a {@link Table} bean. It is also used by {@link SQLite4JavaOutputFormat}.
 */
@SuppressWarnings("serial")
public class TableSpec implements Serializable {

  private final Schema schema;
  private final Field[] partitionFields;
  private final FieldIndex[] indexes;
  private final String partitionByJavaScript;
  private final String[] initialSQL;
  private final String[] preInsertsSQL;
  private final String[] postInsertsSQL;
  private final String[] finalSQL;
  private transient final OrderBy insertionOrderBy;

  /**
   * Simple TableSpec constructor that creates one TableSpec for a schema with a single partition field that is also indexed.
   * Warning: an index is created automatically for the partition field.
   */
  public TableSpec(Schema schema, Field partitionField) {
    this(schema, new Field[]{partitionField}, new FieldIndex[]{new FieldIndex(partitionField)}, null, null, null, null, null);
  }

  /**
   * Creates a Table specification.
   *
   * @param schema           The schema that defines the table
   * @param partitionFields  fields to partition the table by
   * @param indexes          The indexes that mus be created
   * @param initialSQL       SQL statements that will be executed at the start of the process, just after
   *                         some default PRAGMA statements and just before the CREATE TABLE statements.
   * @param preInsertsSQL    SQL statements that will be executed just after the CREATE TABLE statements
   *                         but just before the INSERT statements used to insert data.
   * @param postInsertsSQL   SQL statements that will be executed just after all data is inserted but
   *                         just before the CREATE INDEX statements.
   * @param finalSQL         SQL statements that will be executed al the end of the process, just after the
   *                         CREATE INDEX statements.
   * @param insertionOrderBy The order in which data is inserted in the database. That is very important
   *                         because affect data locality and some queries could go faster or very
   *                         slow depending on the order the data is stored. Usually, data should be
   *                         sorted in the same order than the main index that you will use for queries.
   *                         As a common rule, sort data in the proper order to answer the most important
   *                         queries of your system.
   */
  public TableSpec(Schema schema, Field[] partitionFields, FieldIndex[] indexes,
                   String[] initialSQL, String[] preInsertsSQL, String[] postInsertsSQL,
                   String[] finalSQL, OrderBy insertionOrderBy) {
    this.schema = schema;
    this.partitionFields = partitionFields;
    this.indexes = indexes;
    this.partitionByJavaScript = null;
    this.initialSQL = initialSQL;
    this.preInsertsSQL = preInsertsSQL;
    this.postInsertsSQL = postInsertsSQL;
    this.finalSQL = finalSQL;
    this.insertionOrderBy = insertionOrderBy;
  }

  /**
   * Creates a Table specification.
   *
   * @param schema                The schema that defines the table
   * @param partitionByJavaScript JavaScript function that applies to rows and returns a key that must
   *                              be used to partition.
   * @param indexes               The indexes that mus be created
   * @param initialSQL            SQL statements that will be executed at the start of the process, just after
   *                              some default PRAGMA statements and just before the CREATE TABLE statements.
   * @param preInsertsSQL         SQL statements that will be executed just after the CREATE TABLE statements
   *                              but just before the INSERT statements used to insert data.
   * @param postInsertsSQL        SQL statements that will be executed just after all data is inserted but
   *                              just before the CREATE INDEX statements.
   * @param finalSQL              SQL statements that will be executed al the end of the process, just after the
   *                              CREATE INDEX statements.
   * @param insertionOrderBy      The order in which data is inserted in the database. That is very important
   *                              because affect data locality and some queries could go faster or very
   *                              slow depending on the order the data is stored. Usually, data should be
   *                              sorted in the same order than the main index that you will use for queries.
   *                              As a common rule, sort data in the proper order to answer the most important
   *                              queries of your system.
   */
  public TableSpec(Schema schema, String partitionByJavaScript, FieldIndex[] indexes,
                   String[] initialSQL, String[] preInsertsSQL, String[] postInsertsSQL,
                   String[] finalSQL, OrderBy insertionOrderBy) {
    this.schema = schema;
    this.partitionFields = null;
    this.partitionByJavaScript = partitionByJavaScript;
    this.indexes = indexes;
    this.initialSQL = initialSQL;
    this.preInsertsSQL = preInsertsSQL;
    this.postInsertsSQL = postInsertsSQL;
    this.finalSQL = finalSQL;
    this.insertionOrderBy = insertionOrderBy;
  }

  /**
   * A database index made up by one or more Pangool Fields.
   */
  public static class FieldIndex implements Serializable {

    private Field[] fields;

    public FieldIndex(Field field) {
      this.fields = new Field[]{field};
    }

    public FieldIndex(Field... fields) {
      this.fields = fields;
    }

    public Field[] getIndexFields() {
      return fields;
    }

    @Override
    public String toString() {
      return Arrays.toString(fields);
    }
  }

  // ---- Getters ---- //

  public Schema getSchema() {
    return schema;
  }

  public FieldIndex[] getIndexes() {
    return indexes;
  }

  public Field[] getPartitionFields() {
    return partitionFields;
  }

  public String getPartitionByJavaScript() {
    return partitionByJavaScript;
  }

  public String[] getPostInsertsSQL() {
    return postInsertsSQL;
  }

  public String[] getInitialSQL() {
    return initialSQL;
  }

  public String[] getFinalSQL() {
    return finalSQL;
  }

  public String[] getPreInsertsSQL() {
    return preInsertsSQL;
  }

  public OrderBy getInsertionOrderBy() {
    return insertionOrderBy;
  }
}