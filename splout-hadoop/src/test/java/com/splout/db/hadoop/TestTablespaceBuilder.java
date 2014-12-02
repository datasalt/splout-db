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
import com.splout.db.hadoop.TableBuilder.TableBuilderException;
import com.splout.db.hadoop.TablespaceBuilder.TablespaceBuilderException;
import org.junit.Test;

public class TestTablespaceBuilder {

  private static Schema SCHEMA_1 = new Schema("schema1", Fields.parse("id1:string, value1:string"));
  private static Schema SCHEMA_2 = new Schema("schema2", Fields.parse("id2:string, value2:string"));
  private static Schema SCHEMA_3 = new Schema("schema3", Fields.parse("id3:int, value3:string"));

  @Test
  public void testCorrectTablespace() throws TableBuilderException, TablespaceBuilderException {
    Table table1 = new TableBuilder(SCHEMA_1).addCSVTextFile("foo1.txt").partitionBy("id1").build();
    Table table2 = new TableBuilder(SCHEMA_2).addCSVTextFile("foo2.txt").partitionBy("id2").build();

    TablespaceBuilder builder = new TablespaceBuilder();
    builder.add(table1);
    builder.add(table2);

    builder.setNPartitions(2);

    builder.build();
  }

  @Test
  public void testCorrectTablespaceMultiplePartitionBy() throws TableBuilderException, TablespaceBuilderException {
    Table table1 = new TableBuilder(SCHEMA_1).addCSVTextFile("foo1.txt").partitionBy("id1", "value1").build();
    Table table2 = new TableBuilder(SCHEMA_2).addCSVTextFile("foo2.txt").partitionBy("id2", "value2").build();

    TablespaceBuilder builder = new TablespaceBuilder();
    builder.add(table1);
    builder.add(table2);

    builder.setNPartitions(2);

    builder.build();
  }

  @Test
  public void testCorrectTablespaceWithReplicated() throws TableBuilderException, TablespaceBuilderException {
    Table table1 = new TableBuilder(SCHEMA_1).addCSVTextFile("foo1.txt").partitionBy("id1").build();
    Table table2 = new TableBuilder(SCHEMA_2).addCSVTextFile("foo2.txt").partitionBy("id2").build();
    Table table3 = new TableBuilder(SCHEMA_3).addCSVTextFile("foo3.txt").replicateToAll().build();

    TablespaceBuilder builder = new TablespaceBuilder();
    builder.add(table1);
    builder.add(table2);
    builder.add(table3);

    builder.setNPartitions(2);

    builder.build();
  }

  // ---- Number of partitions must be specified for Tablespace ---- //

  @Test(expected = TablespaceBuilderException.class)
  public void testMissingNPartitions() throws TableBuilderException, TablespaceBuilderException {
    Table table1 = new TableBuilder(SCHEMA_1).addCSVTextFile("foo1.txt").partitionBy("id1").build();
    Table table2 = new TableBuilder(SCHEMA_2).addCSVTextFile("foo2.txt").partitionBy("id2").build();

    TablespaceBuilder builder = new TablespaceBuilder();
    builder.add(table1);
    builder.add(table2);

    builder.build();
  }

  // ---- At least one table must be partitioned ---- //

  @Test(expected = TablespaceBuilderException.class)
  public void testNoPartitionTable() throws TableBuilderException, TablespaceBuilderException {
    Table table1 = new TableBuilder(SCHEMA_1).addCSVTextFile("foo1.txt").replicateToAll().build();
    Table table2 = new TableBuilder(SCHEMA_2).addCSVTextFile("foo2.txt").replicateToAll().build();

    TablespaceBuilder builder = new TablespaceBuilder();
    builder.add(table1);
    builder.add(table2);

    builder.setNPartitions(2);

    builder.build();
  }

  // ---- There must not be collision between table names (schema names) ---- //

  @Test(expected = TablespaceBuilderException.class)
  public void testTableNameCollision() throws TableBuilderException, TablespaceBuilderException {
    Table table1 = new TableBuilder(SCHEMA_1).addCSVTextFile("foo1.txt").partitionBy("id1").build();
    Table table2 = new TableBuilder(SCHEMA_1).addCSVTextFile("foo2.txt").partitionBy("id1").build();

    TablespaceBuilder builder = new TablespaceBuilder();
    builder.add(table1);
    builder.add(table2);

    builder.setNPartitions(2);

    builder.build();
  }

  // ---- Partition field must be of same kind across tables ---- //

  @Test(expected = TablespaceBuilderException.class)
  public void testInvalidCoPartition() throws TableBuilderException, TablespaceBuilderException {
    Table table1 = new TableBuilder(SCHEMA_1).addCSVTextFile("foo1.txt").partitionBy("id1").build();
    Table table2 = new TableBuilder(SCHEMA_3).addCSVTextFile("foo2.txt").partitionBy("id3").build();

    TablespaceBuilder builder = new TablespaceBuilder();
    builder.add(table1);
    builder.add(table2);

    builder.setNPartitions(2);

    builder.build();
  }

  @Test(expected = TablespaceBuilderException.class)
  public void testInvalidCoPartitionMultipleFields() throws TableBuilderException, TablespaceBuilderException {
    Table table1 = new TableBuilder(SCHEMA_1).addCSVTextFile("foo1.txt").partitionBy("value1", "id1").build();
    Table table2 = new TableBuilder(SCHEMA_3).addCSVTextFile("foo2.txt").partitionBy("value3", "id3").build();

    TablespaceBuilder builder = new TablespaceBuilder();
    builder.add(table1);
    builder.add(table2);

    builder.setNPartitions(2);

    builder.build();
  }
}
