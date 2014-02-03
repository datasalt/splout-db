package com.splout.db.hadoop.engine;

/*
 * #%L
 * Splout SQL Hadoop library
 * %%
 * Copyright (C) 2012 - 2014 Datasalt Systems S.L.
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
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.tuplemr.IdentityTupleReducer;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HadoopInputFormat;
import com.splout.db.engine.OutputFormatFactory;
import com.splout.db.engine.SploutEngine;
import com.splout.db.hadoop.NullableSchema;
import com.splout.db.hadoop.TableSpec;

/**
 * A very simple base code for testing {@link SploutSQLOutputFormat} classes.
 * To be reused or to be used as guide for other tests.
 */
@SuppressWarnings("serial")
public class SploutSQLOutputFormatTester implements Serializable {

	public final static String INPUT1 = "in1-" + SploutSQLOutputFormatTester.class.getName();
	public final static String OUTPUT = "out-" + SploutSQLOutputFormatTester.class.getName();

	final Schema tupleSchema1 = new Schema("schema1", Fields.parse("a:string, b:int"));

	public Schema getTupleSchema1() {
	  return tupleSchema1;
  }
	
	/**
	 * Executes a MapReduce Job that uses the outputformat of the given Engine and writes some foo data.
	 * The data written has two fields: a string field ("a") and an int field ("b"). 
	 * <p>
	 * The data is partitioned by the string field ("a").
	 * <p>
	 * The table name is "schema1". The values in column "a" go from "foo1" to "foo6".
	 * The values in column "b" are 30, 20, 140, 110, 220 and 260.
	 */
  protected void runTest(SploutEngine engine) throws Exception {
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

		List<Field> fields = new ArrayList<Field>();
		fields.addAll(tupleSchema1.getFields());
		fields.add(Field.create(SQLite4JavaOutputFormat.PARTITION_TUPLE_FIELD, Schema.Field.Type.INT));
		final Schema metaSchema1 = new Schema("schema1", fields);

		TupleMRBuilder builder = new TupleMRBuilder(new Configuration());
		builder.addIntermediateSchema(NullableSchema.nullableSchema(metaSchema1));

		builder.addInput(new Path(INPUT1), new HadoopInputFormat(TextInputFormat.class),
		    new TupleMapper<LongWritable, Text>() {

			    ITuple tupleInTuple1 = new Tuple(metaSchema1);

			    @Override
			    public void map(LongWritable key, Text value, TupleMRContext context, Collector collector)
			        throws IOException, InterruptedException {
				    String[] split = value.toString().split("\t");
				    tupleInTuple1.set("a", split[0]);
				    tupleInTuple1.set("b", Integer.parseInt(split[1]));
				    tupleInTuple1.set(SploutSQLOutputFormat.PARTITION_TUPLE_FIELD, 0);
				    collector.write(tupleInTuple1);
			    }
		    });

		TableSpec table1 = new TableSpec(tupleSchema1, tupleSchema1.getField(0));
 			
		builder.setTupleReducer(new IdentityTupleReducer());
		builder.setGroupByFields(SploutSQLOutputFormat.PARTITION_TUPLE_FIELD);
		builder.setOutput(new Path(OUTPUT), OutputFormatFactory.getOutputFormat(engine, 10000, new TableSpec[] { table1 }),
		    ITuple.class, NullWritable.class);

		Job job = builder.createJob();
		try {
			job.waitForCompletion(true);
		} finally {
			builder.cleanUpInstanceFiles();
		}
	}
	
	@AfterClass
	@BeforeClass
	public static void cleanup() throws IOException, InterruptedException {
		Runtime.getRuntime().exec("rm -rf " + INPUT1).waitFor();
		Runtime.getRuntime().exec("rm -rf " + OUTPUT).waitFor();
	}	
}
