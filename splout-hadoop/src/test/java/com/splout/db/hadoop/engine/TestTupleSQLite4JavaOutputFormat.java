package com.splout.db.hadoop.engine;

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

import static org.junit.Assert.assertEquals;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.junit.Test;

import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.tuplemr.IdentityTupleReducer;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HadoopInputFormat;
import com.splout.db.common.JSONSerDe;
import com.splout.db.common.JSONSerDe.JSONSerDeException;
import com.splout.db.common.engine.EngineManager.EngineException;
import com.splout.db.common.engine.SQLite4JavaManager;
import com.splout.db.hadoop.NullableSchema;
import com.splout.db.hadoop.TableSpec;
import com.splout.db.hadoop.TableSpec.FieldIndex;
import com.splout.db.hadoop.engine.TupleSQLite4JavaOutputFormat;
import com.splout.db.hadoop.engine.TupleSQLite4JavaOutputFormat.TupleSQLiteOutputFormatException;

@SuppressWarnings("serial")
public class TestTupleSQLite4JavaOutputFormat implements Serializable {

	public final static String INPUT1 = "in1-" + TestTupleSQLite4JavaOutputFormat.class.getName();
	public final static String INPUT2 = "in2-" + TestTupleSQLite4JavaOutputFormat.class.getName();
	public final static String OUTPUT = "out-" + TestTupleSQLite4JavaOutputFormat.class.getName();
	
	@Test
	public void testPreSQL() throws Exception {
		final Schema tupleSchema1 = new Schema("schema1", Fields.parse("a:string, b:int"));
    String[] initSQL = new String[]{"init1", "init2"};
    String[] preInsertSQL = new String[]{"CREATE Mytable;", "ME_LO_INVENTO"};
    TableSpec tableSpec = new TableSpec(tupleSchema1, new Field[]{tupleSchema1.getField(0)},
        new FieldIndex[]{new FieldIndex(tupleSchema1.getField(0), tupleSchema1.getField(1))},
        initSQL, preInsertSQL, null, null, null);
    String[] createTables = TupleSQLite4JavaOutputFormat.getCreateTables(tableSpec);
    assertEquals("init1", createTables[0]);
    assertEquals("init2", createTables[1]);
    assertEquals("CREATE TABLE schema1 (a TEXT, b INTEGER);", createTables[2]);
    assertEquals("CREATE Mytable;", createTables[3]);
    assertEquals("ME_LO_INVENTO", createTables[4]);
  }
	
	@Test
	public void testPostSQL() throws Exception {
		final Schema tupleSchema1 = new Schema("schema1", Fields.parse("a:string, b:int"));
    String[] afterInsertSQL = new String[]{"afterinsert1", "afterinsert2"};
    String[] finalSQL = new String[]{"DROP INDEX idx_schema1_ab", "CREATE INDEX blablabla"};
    TableSpec tableSpec = new TableSpec(tupleSchema1, new Field[]{tupleSchema1.getField(0)},
        new FieldIndex[]{new FieldIndex(tupleSchema1.getField(0), tupleSchema1.getField(1))},
        null, null, afterInsertSQL, finalSQL, null);
    String[] createIndex = TupleSQLite4JavaOutputFormat.getCreateIndexes(tableSpec);
    assertEquals("afterinsert1", createIndex[0]);
    assertEquals("afterinsert2", createIndex[1]);
    assertEquals("CREATE INDEX idx_schema1_ab ON schema1(a, b);", createIndex[2]);
    assertEquals("DROP INDEX idx_schema1_ab", createIndex[3]);
    assertEquals("CREATE INDEX blablabla", createIndex[4]);
  }
	
	@Test
	public void testCompoundIndexes() throws TupleSQLiteOutputFormatException {
		final Schema tupleSchema1 = new Schema("schema1", Fields.parse("a:string, b:int"));
    TableSpec tableSpec = new TableSpec(tupleSchema1, new Field[]{tupleSchema1.getField(0)}, new FieldIndex[]{new FieldIndex(tupleSchema1.getField(0), tupleSchema1.getField(1))}, null, null, null, null, null);
    String[] createIndex = TupleSQLite4JavaOutputFormat.getCreateIndexes(tableSpec);
		assertEquals("CREATE INDEX idx_schema1_ab ON schema1(a, b);", createIndex[0]);
	}
	
	@SuppressWarnings("rawtypes")
  @Test
	public void test() throws IOException, TupleMRException, InterruptedException, ClassNotFoundException, SQLException,
	    JSONSerDeException, TupleSQLiteOutputFormatException, EngineException {

		Runtime.getRuntime().exec("rm -rf " + OUTPUT);

		// Prepare input
		BufferedWriter writer;
		
		writer = new BufferedWriter(new FileWriter(INPUT1));
		writer.write("foo1" + "\t" + "30" + "\n");
		writer.write("foo2" + "\t" + "20" + "\n");
		writer.write("foo3" + "\t" + "140" + "\n");
		writer.write("foo4" + "\t" + "110" + "\n");
		writer.write("foo5" + "\t" + "220" + "\n");
		writer.write("foo6" + "\t" + "260" + "\n");
		writer.close();

		writer = new BufferedWriter(new FileWriter(INPUT2));
		writer.write("4.5" + "\t" + "true" + "\n");
		writer.write("4.6" + "\t" + "false" + "\n");
		writer.close();
		
		final Schema tupleSchema1 = new Schema("schema1", Fields.parse("a:string, b:int"));
		final Schema tupleSchema2 = new Schema("schema2", Fields.parse("c:double, d:boolean"));
		
		List<Field> fields = new ArrayList<Field>();
		fields.addAll(tupleSchema1.getFields());
		fields.add(Field.create(TupleSQLite4JavaOutputFormat.PARTITION_TUPLE_FIELD, Schema.Field.Type.INT));
		final Schema metaSchema1  = new Schema("schema1", fields);
		
		fields.clear();
		fields.addAll(tupleSchema2.getFields());
		fields.add(Field.create(TupleSQLite4JavaOutputFormat.PARTITION_TUPLE_FIELD, Schema.Field.Type.INT));
		final Schema metaSchema2  = new Schema("schema2", fields);
		
		TupleMRBuilder builder = new TupleMRBuilder(new Configuration());
		builder.addIntermediateSchema(NullableSchema.nullableSchema(metaSchema1));
		builder.addIntermediateSchema(NullableSchema.nullableSchema(metaSchema2));
		builder.addInput(new Path(INPUT1), new HadoopInputFormat(TextInputFormat.class),
		    new TupleMapper<LongWritable, Text>() {

			    ITuple tupleInTuple1 = new Tuple(metaSchema1);

			    @Override
			    public void map(LongWritable key, Text value, TupleMRContext context, Collector collector)
			        throws IOException, InterruptedException {
				    String[] split = value.toString().split("\t");
				    tupleInTuple1.set("a", split[0]);
				    tupleInTuple1.set("b", Integer.parseInt(split[1]));
				    tupleInTuple1.set(TupleSQLite4JavaOutputFormat.PARTITION_TUPLE_FIELD, 0);
				    collector.write(tupleInTuple1);
			    }
		    });

		builder.addInput(new Path(INPUT2), new HadoopInputFormat(TextInputFormat.class),
		    new TupleMapper<LongWritable, Text>() {

			    ITuple tupleInTuple2 = new Tuple(metaSchema2);

			    @Override
			    public void map(LongWritable key, Text value, TupleMRContext context, Collector collector)
			        throws IOException, InterruptedException {
				    String[] split = value.toString().split("\t");
				    tupleInTuple2.set("c", Double.parseDouble(split[0]));
				    tupleInTuple2.set("d", Boolean.parseBoolean(split[1]));
				    tupleInTuple2.set(TupleSQLite4JavaOutputFormat.PARTITION_TUPLE_FIELD, 0);
				    collector.write(tupleInTuple2);
			    }
		    });
		
		TableSpec table1 = new TableSpec(tupleSchema1, tupleSchema1.getField(0));
		TableSpec table2 = new TableSpec(tupleSchema2, tupleSchema2.getField(0));
		builder.setTupleReducer(new IdentityTupleReducer());
		builder.setGroupByFields(TupleSQLite4JavaOutputFormat.PARTITION_TUPLE_FIELD);
		builder.setOutput(new Path(OUTPUT), new TupleSQLite4JavaOutputFormat(100000, table1, table2), ITuple.class, NullWritable.class);
		
		Job job = builder.createJob();
		try {
			job.waitForCompletion(true);
		} finally {
			builder.cleanUpInstanceFiles();
		}

		// Assert that the DB has been created successfully

		SQLite4JavaManager manager = new SQLite4JavaManager(OUTPUT + "/0.db", null);
		List list = JSONSerDe.deSer(manager.query("SELECT * FROM schema1;", 100), ArrayList.class);
		assertEquals(6, list.size());
		list = JSONSerDe.deSer(manager.query("SELECT * FROM schema2;", 100), ArrayList.class);
		assertEquals(2, list.size());
		manager.close();

		Runtime.getRuntime().exec("rm -rf " + INPUT1);
		Runtime.getRuntime().exec("rm -rf " + INPUT2);
		Runtime.getRuntime().exec("rm -rf " + OUTPUT);

	}
}
