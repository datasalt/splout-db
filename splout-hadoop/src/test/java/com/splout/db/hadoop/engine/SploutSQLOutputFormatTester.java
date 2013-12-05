package com.splout.db.hadoop.engine;

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
import com.splout.db.hadoop.NullableSchema;
import com.splout.db.hadoop.TableSpec;

@SuppressWarnings("serial")
public class SploutSQLOutputFormatTester implements Serializable {

	public final static String INPUT1 = "in1-" + SploutSQLOutputFormatTester.class.getName();
	public final static String INPUT2 = "in2-" + SploutSQLOutputFormatTester.class.getName();
	public final static String OUTPUT = "out-" + SploutSQLOutputFormatTester.class.getName();
	
  protected void runTest(Class<? extends SploutSQLOutputFormat> outputFormatClass) throws Exception {
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
		fields.add(Field.create(SQLite4JavaOutputFormat.PARTITION_TUPLE_FIELD, Schema.Field.Type.INT));
		final Schema metaSchema1 = new Schema("schema1", fields);

		fields.clear();
		fields.addAll(tupleSchema2.getFields());
		fields.add(Field.create(SQLite4JavaOutputFormat.PARTITION_TUPLE_FIELD, Schema.Field.Type.INT));
		final Schema metaSchema2 = new Schema("schema2", fields);

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
				    tupleInTuple1.set(SploutSQLOutputFormat.PARTITION_TUPLE_FIELD, 0);
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
				    tupleInTuple2.set(SploutSQLOutputFormat.PARTITION_TUPLE_FIELD, 0);
				    collector.write(tupleInTuple2);
			    }
		    });

		TableSpec table1 = new TableSpec(tupleSchema1, tupleSchema1.getField(1));
		TableSpec table2 = new TableSpec(tupleSchema2, tupleSchema2.getField(0));
		
		SploutSQLOutputFormat outputFormat = null;
		
		if(outputFormatClass.getName().equals(SQLite4JavaOutputFormat.class.getName())) {
			outputFormat = new SQLite4JavaOutputFormat(100000, table1, table2);			
		} else if(outputFormatClass.getName().equals(MySQLOutputFormat.class.getName())) {
			outputFormat = new MySQLOutputFormat(100000, table1, table2);
		}
		
		builder.setTupleReducer(new IdentityTupleReducer());
		builder.setGroupByFields(SploutSQLOutputFormat.PARTITION_TUPLE_FIELD);
		builder.setOutput(new Path(OUTPUT), new SploutSQLProxyOutputFormat(outputFormat),
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
		Runtime.getRuntime().exec("rm -rf " + INPUT2).waitFor();
		Runtime.getRuntime().exec("rm -rf " + OUTPUT).waitFor();
	}
	
}
