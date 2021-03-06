package com.splout.db.hadoop.engine;

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

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.splout.db.hadoop.TableSpec;
import com.splout.db.hadoop.TableSpec.FieldIndex;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Abstract class that can be extended for generating arbitrary outputformats in Splout.
 */
@SuppressWarnings("serial")
public abstract class SploutSQLOutputFormat implements Serializable, Configurable {

  public final static String PARTITION_TUPLE_FIELD = "_partition";

  public static class SploutSQLOutputFormatException extends Exception {

    public SploutSQLOutputFormatException(String cause) {
      super(cause);
    }

    public SploutSQLOutputFormatException(String cause, Exception e) {
      super(cause, e);
    }
  }

  public abstract String getCreateTable(TableSpec tableSpec) throws SploutSQLOutputFormatException;

  public abstract void initPartition(int partition, Path localDbFile) throws IOException, InterruptedException;

  public abstract void write(ITuple tuple) throws IOException, InterruptedException;

  public abstract void close() throws IOException, InterruptedException;

  private int batchSize;
  private TableSpec[] dbSpec;
  private transient Configuration conf;

  /**
   * This OutputFormat receives a list of {@link TableSpec}. These are the different tables that will be created. They
   * will be identified by Pangool Tuples. The batch size is the number of SQL statements to execute before a COMMIT.
   */
  public SploutSQLOutputFormat(Integer batchSize, TableSpec... dbSpec) throws SploutSQLOutputFormatException {
    this.batchSize = batchSize;
    this.dbSpec = dbSpec;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public String[] getPostSQL() throws SploutSQLOutputFormatException {
    return getCreateIndexes(dbSpec);
  }

  public String[] getPreSQL() throws SploutSQLOutputFormatException {
    return getCreateTables(dbSpec);
  }

  public static Field getPartitionField() {
    return Field.create(SploutSQLOutputFormat.PARTITION_TUPLE_FIELD, Type.INT);
  }

  // Get all the CREATE TABLE... for a list of {@link TableSpec}
  protected String[] getCreateTables(TableSpec... tableSpecs) throws SploutSQLOutputFormatException {
    List<String> createTables = new ArrayList<String>();
    // First the initSQL provided by user
    for (TableSpec tableSpec : tableSpecs) {
      if (tableSpec.getInitialSQL() != null) {
        createTables.addAll(Arrays.asList(tableSpec.getInitialSQL()));
      }
    }
    // CREATE TABLE statements
    for (TableSpec tableSpec : tableSpecs) {
      createTables.add(getCreateTable(tableSpec));
    }
    // Add user preInsertsSQL if exists just after the CREATE TABLE's
    for (TableSpec tableSpec : tableSpecs) {
      if (tableSpec.getPreInsertsSQL() != null) {
        createTables.addAll(Arrays.asList(tableSpec.getPreInsertsSQL()));
      }
    }
    return createTables.toArray(new String[0]);
  }

  // Get a list of CREATE INDEX... Statements for a {@link TableSpec} list.
  protected static String[] getCreateIndexes(TableSpec... tableSpecs)
      throws SploutSQLOutputFormatException {
    List<String> createIndexes = new ArrayList<String>();
    // Add user postInsertsSQL if exists just before the CREATE INDEX statements
    for (TableSpec tableSpec : tableSpecs) {
      if (tableSpec.getPostInsertsSQL() != null) {
        createIndexes.addAll(Arrays.asList(tableSpec.getPostInsertsSQL()));
      }
    }
    for (TableSpec tableSpec : tableSpecs) {
      for (FieldIndex index : tableSpec.getIndexes()) {
        for (Field field : index.getIndexFields()) {
          if (!tableSpec.getSchema().getFields().contains(field)) {
            throw new SploutSQLOutputFormatException("Field to index (" + index
                + ") not contained in input schema (" + tableSpec.getSchema() + ")");
          }
        }
        // The following code is able to create indexes for one field or for multiple fields
        String createIndex = "CREATE INDEX idx_" + tableSpec.getSchema().getName() + "_";
        for (Field field : index.getIndexFields()) {
          createIndex += field.getName();
        }
        createIndex += " ON " + tableSpec.getSchema().getName() + "(";
        for (Field field : index.getIndexFields()) {
          createIndex += "`" + field.getName() + "`, ";
        }
        createIndex = createIndex.substring(0, createIndex.length() - 2) + ");";
        createIndexes.add(createIndex);
      }
    }
    // Add user finalSQL if exists just after the CREATE INDEX statements
    for (TableSpec tableSpec : tableSpecs) {
      if (tableSpec.getFinalSQL() != null) {
        createIndexes.addAll(Arrays.asList(tableSpec.getFinalSQL()));
      }
    }
    return createIndexes.toArray(new String[0]);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
