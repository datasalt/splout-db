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

import com.datasalt.pangool.io.*;
import com.datasalt.pangool.tuplemr.Criteria;
import com.datasalt.pangool.tuplemr.OrderBy;
import com.datasalt.pangool.tuplemr.mapred.lib.input.TupleInputFormat;
import com.datasalt.pangool.utils.test.AbstractHadoopTestLibrary;
import com.splout.db.common.PartitionEntry;
import com.splout.db.common.SQLiteJDBCManager;
import com.splout.db.hadoop.TupleSampler.SamplingType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

@SuppressWarnings({ "rawtypes", "serial" })
public class TestTablespaceGenerator extends AbstractHadoopTestLibrary implements Serializable  {

	public final static String INPUT  = "in-"  + TestTablespaceGenerator.class.getName();
	public final static String OUTPUT = "out-" + TestTablespaceGenerator.class.getName();
	static Schema theSchema1 = new Schema("schema1", Fields.parse("id:string, value:string"));
	static Schema theSchema2 = new Schema("schema2", Fields.parse("id:string, value:string, intValue:int, doubleValue:double, strValue:string"));


  @Test
	public void simpleTest() throws Exception {
    trash(INPUT, OUTPUT);

		Configuration conf = new Configuration();
		TupleFile.Writer writer = new TupleFile.Writer(FileSystem.get(conf), conf, new Path(INPUT), theSchema1);

    writer.append(getTuple("id1", "value12"));
    writer.append(getTuple("id1", "value11"));
    writer.append(getTuple("id1", "value13"));
		writer.append(getTuple("id1", "value14"));

		writer.append(getTuple("id2", "value21"));
		writer.append(getTuple("id2", "value22"));
		writer.append(getTuple("id3", "value31"));
		writer.append(getTuple("id4", "value41"));

		writer.append(getTuple("id5", "value51"));
		writer.append(getTuple("id5", "value52"));
		writer.append(getTuple("id6", "value53"));
		writer.append(getTuple("id6", "value54"));
		writer.append(getTuple("id7", "value55"));
		writer.append(getTuple("id7", "value56"));

		writer.append(getTuple("id8", "value61"));
		writer.append(getTuple("id8", "value62"));
		
		writer.close();
		
		TablespaceSpec tablespace = TablespaceSpec.of(theSchema1, "id", new Path(INPUT), new TupleInputFormat(),  4);
		TablespaceGenerator viewGenerator = new TablespaceGenerator(tablespace, new Path(OUTPUT), this.getClass());
		viewGenerator.generateView(conf, SamplingType.DEFAULT, new TupleSampler.DefaultSamplingOptions());
		
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
    trash(INPUT, OUTPUT);

		Configuration conf = new Configuration();
    TupleFile.Writer writer = new TupleFile.Writer(FileSystem.get(conf), conf, new Path(INPUT), NullableSchema.nullableSchema(theSchema2));

		writer.append(getTupleWithNulls("id1", "value11", null, -1.0, null));
		writer.append(getTupleWithNulls("id2", "value12", null, null, "Hello"));
		writer.append(getTupleWithNulls("id3", "value13", 100, null, "Hello"));
		writer.append(getTupleWithNulls("id4", "value14", 100, 2.0, ""));
		writer.append(getTupleWithNulls("id5", "value15", 100, 2.0, null));
		
		writer.close();
		
		TablespaceSpec tablespace = TablespaceSpec.of(theSchema2, "id", new Path(INPUT), new TupleInputFormat(), 1);
		TablespaceGenerator viewGenerator = new TablespaceGenerator(tablespace, new Path(OUTPUT), this.getClass());
		viewGenerator.generateView(conf, SamplingType.DEFAULT, new TupleSampler.DefaultSamplingOptions());
		
		SQLiteJDBCManager manager = new SQLiteJDBCManager(OUTPUT + "/store/0.db", 10);
    String results = manager.query("SELECT * FROM schema2;", 100);
		assertTrue(results.contains("null"));

    assertNull(searchRow(results, "id", "id1").get("intValue"));
    assertEquals(-1.0, searchRow(results, "id", "id1").get("doubleValue"));
    assertNull(searchRow(results, "id", "id1").get("strValue"));

    assertNull(searchRow(results, "id", "id2").get("intValue"));
    assertNull(searchRow(results, "id", "id2").get("doubleValue"));
    assertEquals("Hello", searchRow(results, "id", "id2").get("strValue"));

    assertEquals(100, searchRow(results, "id", "id3").get("intValue"));
    assertNull(searchRow(results, "id", "id3").get("doubleValue"));
    assertEquals("Hello", searchRow(results, "id", "id3").get("strValue"));

    assertEquals(100, searchRow(results, "id", "id4").get("intValue"));
    assertEquals(2.0, searchRow(results, "id", "id4").get("doubleValue"));
    assertEquals("", searchRow(results, "id", "id4").get("strValue"));

    assertEquals(100, searchRow(results, "id", "id5").get("intValue"));
    assertEquals(2.0, searchRow(results, "id", "id5").get("doubleValue"));
    assertNull(searchRow(results, "id", "id5").get("strValue"));

    trash(INPUT, OUTPUT);
  }

  @Test
  public void testRecordProcessor() throws Exception {
    testRecordProcessor(false);
  }

  @Test
  public void testRecordProcessorReplicateAll() throws Exception {
    testRecordProcessor(true);
  }

  public void testRecordProcessor(boolean replicateAll) throws Exception {
    int TUPLES_TO_GENERATE = 10;

    trash(INPUT, INPUT + 2, OUTPUT);

    Configuration conf = new Configuration();
    TupleFile.Writer writer = new TupleFile.Writer(FileSystem.get(conf), conf, new Path(INPUT),
        NullableSchema.nullableSchema(theSchema1));

    for(int i=0; i<TUPLES_TO_GENERATE; i++) {
      writer.append(getTuple("id" + i, "str" + i));
    }

    writer.close();

    // Dummy table.
    writer = new TupleFile.Writer(FileSystem.get(conf), conf, new Path(INPUT+2),
        NullableSchema.nullableSchema(theSchema1));
    writer.append(getTuple("dummy", "dummy"));
    writer.close();

    TablespaceBuilder builder = new TablespaceBuilder();
    TableBuilder tBuilder = new TableBuilder(theSchema1);
    tBuilder.addTupleFile(new Path(INPUT), new RecordProcessor() {
      @Override
      public ITuple process(ITuple record, CounterInterface context) throws Throwable {
        context.getCounter("counter", "counter").increment(1);
        ((Utf8) record.get("id")).set(record.get("id") + "mod");
        ((Utf8) record.get("value")).set(record.get("value")+"mod");
        return record;
      }
    });
    if (replicateAll) {
      tBuilder.replicateToAll();
    } else {
      tBuilder.partitionBy("id");
    }
    builder.add(tBuilder.build());
    builder.setNPartitions(1);

    // Dummy tabled added only because at least one table with partition must be present
    // in the tablespace.
    tBuilder = new TableBuilder(new Schema("dummy", theSchema1.getFields()));
    tBuilder.addTupleFile(new Path(INPUT + 2));
    tBuilder.partitionBy("id");
    tBuilder.insertionSortOrder(new OrderBy().add("id", Criteria.Order.ASC));
    builder.add(tBuilder.build());

    TablespaceGenerator viewGenerator = new TablespaceGenerator(builder.build(), new Path(OUTPUT), this.getClass());
    viewGenerator.generateView(conf, SamplingType.DEFAULT, new TupleSampler.DefaultSamplingOptions());

    SQLiteJDBCManager manager = new SQLiteJDBCManager(OUTPUT + "/store/0.db", 10);
    String results = manager.query("SELECT * FROM schema1;", TUPLES_TO_GENERATE+1);

    System.out.println(results);
    for(int i=0; i<TUPLES_TO_GENERATE; i++) {
      assertEquals("id" + i + "mod", getVal(results, i, "id"));
      assertEquals("str" + i + "mod", getVal(results, i, "value"));
    }

    trash(INPUT, INPUT + 2, OUTPUT);
  }

  public static Object resultSize(String result) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(result, List.class).size();
  }

  public static Object getVal(String result, int row, String field) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return ((Map)mapper.readValue(result, List.class).get(row)).get(field);
  }

  public static Map searchRow(String result, String field, Object value) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    for (Object o : mapper.readValue(result, List.class)) {
      Map m = (Map) o;
      if (value.equals(m.get(field))) {
        return m;
      }
    }
    return null;
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