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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.junit.Test;

import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.tuplemr.mapred.lib.input.TupleInputFormat;
import com.datasalt.pangool.tuplemr.mapred.lib.output.TupleOutputFormat.TupleRecordWriter;
import com.datasalt.pangool.utils.AvroUtils;
import com.datasalt.pangool.utils.test.AbstractHadoopTestLibrary;
import com.splout.db.common.PartitionEntry;
import com.splout.db.common.SQLiteJDBCManager;
import com.splout.db.hadoop.TupleSampler.SamplingType;

public class TestTablespaceGenerator extends AbstractHadoopTestLibrary {

	public final static String INPUT  = "in-"  + TestTablespaceGenerator.class.getName();
	public final static String OUTPUT = "out-" + TestTablespaceGenerator.class.getName();
	static Schema theSchema1 = new Schema("schema1", Fields.parse("id:string, value:string"));
	static Schema theSchema2 = new Schema("schema2", Fields.parse("id:string, value:string, intValue:int, doubleValue:double, strValue:string"));

  @Test
	public void simpleTest() throws Exception {
		initHadoop();
		trash(INPUT, OUTPUT);
		
		DataFileWriter<Record> avroWriter = new DataFileWriter<Record>(new ReflectDatumWriter<Record>());
		avroWriter.create(AvroUtils.toAvroSchema(theSchema1), new File(INPUT));
		TupleRecordWriter writer = new TupleRecordWriter(theSchema1, avroWriter, getConf());
		
		writer.write(getTuple("id1", "value11"), NullWritable.get());
		writer.write(getTuple("id1", "value12"), NullWritable.get());
		writer.write(getTuple("id1", "value13"), NullWritable.get());
		writer.write(getTuple("id1", "value14"), NullWritable.get());

		writer.write(getTuple("id2", "value21"), NullWritable.get());
		writer.write(getTuple("id2", "value22"), NullWritable.get());
		writer.write(getTuple("id3", "value31"), NullWritable.get());
		writer.write(getTuple("id4", "value41"), NullWritable.get());
		
		writer.write(getTuple("id5", "value51"), NullWritable.get());
		writer.write(getTuple("id5", "value52"), NullWritable.get());
		writer.write(getTuple("id6", "value53"), NullWritable.get());
		writer.write(getTuple("id6", "value54"), NullWritable.get());
		writer.write(getTuple("id7", "value55"), NullWritable.get());
		writer.write(getTuple("id7", "value56"), NullWritable.get());

		writer.write(getTuple("id8", "value61"), NullWritable.get());
		writer.write(getTuple("id8", "value62"), NullWritable.get());
		
		writer.close(null);
		
		TablespaceSpec tablespace = TablespaceSpec.of(theSchema1, "id", new Path(INPUT), new TupleInputFormat(),  4);
		TablespaceGenerator viewGenerator = new TablespaceGenerator(tablespace, new Path(OUTPUT));
		viewGenerator.generateView(getConf(), SamplingType.DEFAULT, new TupleSampler.DefaultSamplingOptions());
		
		List<PartitionEntry> partitionMap = viewGenerator.getPartitionMap().getPartitionEntries();
		assertEquals(4, partitionMap.size());
		
		assertEquals(null, partitionMap.get(0).getMin());
		assertEquals("id2", partitionMap.get(0).getMax());
		assertEquals(0, (int) partitionMap.get(0).getShard());
		
		assertEquals("id2", partitionMap.get(1).getMin());
		assertEquals("id5", partitionMap.get(1).getMax());
		assertEquals(1, (int) partitionMap.get(1).getShard());

		assertEquals("id5", partitionMap.get(2).getMin());
		assertEquals("id7", partitionMap.get(2).getMax());
		assertEquals(2, (int) partitionMap.get(2).getShard());

		assertEquals("id7", partitionMap.get(3).getMin());
		assertEquals(null, partitionMap.get(3).getMax());
		assertEquals(3, (int) partitionMap.get(3).getShard());
		
		trash(INPUT, OUTPUT);
	}
	
  @Test
  public void testAcceptNullValues() throws Exception {
  	initHadoop();
  	
		trash(INPUT, OUTPUT);
		
		DataFileWriter<Record> avroWriter = new DataFileWriter<Record>(new ReflectDatumWriter<Record>());
		avroWriter.create(AvroUtils.toAvroSchema(new NullableSchema(theSchema2)), new File(INPUT));
		TupleRecordWriter writer = new TupleRecordWriter(new NullableSchema(theSchema2), avroWriter, getConf());
		
		writer.write(new NullableTuple(getTupleWithNulls("id1", "value11", null, -1.0, null)), NullWritable.get());
		writer.write(new NullableTuple(getTupleWithNulls("id1", "value12", null, null, "Hello")), NullWritable.get());
		writer.write(new NullableTuple(getTupleWithNulls("id1", "value13", 100, null, "Hello")), NullWritable.get());
		writer.write(new NullableTuple(getTupleWithNulls("id1", "value14", 100, 2.0, "")), NullWritable.get());
		writer.write(new NullableTuple(getTupleWithNulls("id1", "value15", 100, 2.0, null)), NullWritable.get());
		
		writer.close(null);
		
		TablespaceSpec tablespace = TablespaceSpec.of(theSchema2, "id", new Path(INPUT), new TupleInputFormat(), 1);
		TablespaceGenerator viewGenerator = new TablespaceGenerator(tablespace, new Path(OUTPUT));
		viewGenerator.generateView(getConf(), SamplingType.DEFAULT, new TupleSampler.DefaultSamplingOptions());
		
		SQLiteJDBCManager manager = new SQLiteJDBCManager(OUTPUT + "/store/0.db", 10);
		assertTrue(manager.query("SELECT * FROM schema2;", 100).contains("null"));
		
		trash(INPUT, OUTPUT);
  }
  
  public static ITuple getTupleWithNulls(String id, String value, Integer intValue, Double doubleValue, String strValue) {
  	ITuple tuple = new Tuple(theSchema2);
  	tuple.set("id", id);
  	tuple.set("value", value);
  	tuple.set("intValue", intValue);
  	tuple.set("doubleValue", doubleValue);
  	tuple.set("strValue", strValue);
  	return tuple;
  }
  
	public static ITuple getTuple(String id, String value) {
		ITuple tuple = new Tuple(theSchema1);
		tuple.set("id", id);
		tuple.set("value", value);
		return tuple;
	}
}
