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

import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.tuplemr.OrderBy;
import com.splout.db.hadoop.TableBuilder.TableBuilderException;
import org.junit.Test;

public class TestTableBuilder {

  private static Schema SCHEMA = new Schema("schema", Fields.parse("id:string, value:string"));

  // ---- Valid specs ---- //

  @Test
  public void testCorrectReplicatedTable() throws TableBuilderException {
    new TableBuilder(SCHEMA).addCSVTextFile("foo.txt").replicateToAll().build();
  }

  @Test
  public void testCorrectPartitionedTable() throws TableBuilderException {
    new TableBuilder(SCHEMA).addCSVTextFile("foo.txt").partitionBy("id").insertionSortOrder(OrderBy.parse("id:desc, value:desc")).build();
  }

  @Test
  public void testCorrectPartitionedTable2() throws TableBuilderException {
    new TableBuilder(SCHEMA).addCSVTextFile("foo.txt").partitionByJavaScript("function partition(record) { return record.get('foo').toString().substring(0, 2); }").build();
  }

  @Test
  public void testCorrectTableWithIndexes() throws TableBuilderException {
    TableBuilder builder = new TableBuilder(SCHEMA);
    builder.addCSVTextFile("foo.txt");
    builder.partitionBy("id");
    builder.createIndex("value");
    builder.createIndex("id", "value");
    builder.build();
  }

  // ---- Trivial failures (missing mandatory things) ---- //

  @Test(expected = TableBuilderException.class)
  public void testNoPartitionField() throws TableBuilderException {
    new TableBuilder(SCHEMA).addCSVTextFile("foo.txt").build();
  }

  @Test(expected = TableBuilderException.class)
  public void testNoFiles() throws TableBuilderException {
    new TableBuilder(SCHEMA).partitionBy("id").build();
  }

  // ---- Non-trivial failures ---- //

  @Test(expected = TableBuilderException.class)
  public void testInvalidPartitionField() throws TableBuilderException {
    new TableBuilder(SCHEMA).addCSVTextFile("foo.txt").partitionBy("foo").build();
  }

  @Test(expected = TableBuilderException.class)
  public void testInconsistentReplicatedWithPartitionField() throws TableBuilderException {
    new TableBuilder(SCHEMA).addCSVTextFile("foo.txt").partitionBy("id").replicateToAll().build();
  }

  // ---- Invalid indexes ---- //

  @Test(expected = TableBuilderException.class)
  public void testIncorrectIndex() throws TableBuilderException {
    TableBuilder builder = new TableBuilder(SCHEMA);
    builder.addCSVTextFile("foo.txt");
    builder.partitionBy("id");
    builder.createIndex("foo");
    builder.build();
  }

  @Test(expected = TableBuilderException.class)
  public void testIncorrectCompoundIndex() throws TableBuilderException {
    TableBuilder builder = new TableBuilder(SCHEMA);
    builder.addCSVTextFile("foo.txt");
    builder.partitionBy("id");
    builder.createIndex("id", "foo");
    builder.build();
  }

  // --- Invalid JavaScript --- //

  @Test(expected = TableBuilderException.class)
  public void testInvalidJavaScript() throws TableBuilderException {
    new TableBuilder(SCHEMA).addCSVTextFile("foo.txt").partitionByJavaScript("sdfsadf;dfkldkj=0'").build();
  }

  @Test(expected = TableBuilderException.class)
  public void testInvalidJavaScript2() throws TableBuilderException {
    new TableBuilder(SCHEMA).addCSVTextFile("foo.txt").partitionByJavaScript("function myfunct(record) { return \"a\"; }").build();
  }

  @Test(expected = TableBuilderException.class)
  public void testInvalidInsertionSortOrder() throws TableBuilderException {
    new TableBuilder(SCHEMA).addCSVTextFile("foo.txt").partitionBy("id").insertionSortOrder(OrderBy.parse("id:desc, NONEXISTENT:desc")).build();
  }
}
