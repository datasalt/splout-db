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

import static org.junit.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.junit.Test;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.tuplemr.IdentityTupleReducer;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HadoopInputFormat;
import com.splout.db.common.JSONSerDe.JSONSerDeException;
import com.splout.db.common.SQLiteJDBCManager;

@SuppressWarnings("serial")
public class TestSQLiteOutputFormat implements Serializable {

	public final static String INPUT = "in-" + TestSQLiteOutputFormat.class.getName();
	public final static String OUTPUT = "out-" + TestSQLiteOutputFormat.class.getName();

	@Test
	public void test() throws IOException, TupleMRException, InterruptedException, ClassNotFoundException, SQLException,
	    JSONSerDeException {
		Runtime.getRuntime().exec("rm -rf " + INPUT).waitFor();
		Runtime.getRuntime().exec("rm -rf " + OUTPUT).waitFor();

		// Prepare input
		BufferedWriter writer = new BufferedWriter(new FileWriter(INPUT));
		writer.write("INSERT INTO foo (foobar1, foobar2) VALUES (\"foo1\", 10);" + "\n");
		writer.write("INSERT INTO foo (foobar1, foobar2) VALUES (\"foo2\", 20);" + "\n");
		writer.write("INSERT INTO foo (foobar1, foobar2) VALUES (\"foo3\", 30);" + "\n");
		writer.write("INSERT INTO foo (foobar1, foobar2) VALUES (\"foo4\", 40);");
		writer.close();

		TupleMRBuilder builder = new TupleMRBuilder(new Configuration());
		builder.addIntermediateSchema(SQLiteOutputFormat.SCHEMA);
		builder.addInput(new Path(INPUT), new HadoopInputFormat(TextInputFormat.class),
		    new TupleMapper<LongWritable, Text>() {

			    ITuple tuple = new Tuple(SQLiteOutputFormat.SCHEMA);

			    @Override
			    public void map(LongWritable key, Text value, TupleMRContext context, Collector collector)
			        throws IOException, InterruptedException {
				    tuple.set("sql", value.toString());
				    tuple.set("partition", 0);
				    collector.write(tuple);
			    }
		    });
		builder.setGroupByFields("sql");
		builder.setTupleReducer(new IdentityTupleReducer());
		builder.setOutput(new Path(OUTPUT), new SQLiteOutputFormat(
		    new String[] { "CREATE TABLE foo (foobar1 TEXT, foobar2 INT32)" }, 
		    new String[] { "CREATE INDEX idx_foobar1 ON foo (foobar1);" }, 1000000), ITuple.class, NullWritable.class);
		
		
		Job job = builder.createJob();
		try {
			job.waitForCompletion(true);
		} finally{
			builder.cleanUpInstanceFiles();
		}

		// Assert that the DB has been created successfully

		SQLiteJDBCManager manager = new SQLiteJDBCManager(OUTPUT + "/0.db", 10);
		assertTrue(manager.query("SELECT foobar2 FROM foo WHERE foobar1 LIKE \"foo1\";", 100).contains("foobar2\":10"));
		assertTrue(manager.query("SELECT foobar2 FROM foo WHERE foobar1 LIKE \"foo2\";", 100).contains("foobar2\":20"));
		assertTrue(manager.query("SELECT foobar2 FROM foo WHERE foobar1 LIKE \"foo3\";", 100).contains("foobar2\":30"));
		assertTrue(manager.query("SELECT foobar2 FROM foo WHERE foobar1 LIKE \"foo4\";", 100).contains("foobar2\":40"));
		assertTrue(manager.query("SELECT COUNT(*) FROM foo;", 100).contains("COUNT(*)\":4"));
		manager.close();

		Runtime.getRuntime().exec("rm -rf " + INPUT).waitFor();
		Runtime.getRuntime().exec("rm -rf " + OUTPUT).waitFor();
	}
}
