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

import java.util.ArrayList;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;

import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.io.Tuple;

public class TestNullableTuple {

	@Test
	@Ignore
	public void testNoNullableWrapper() {
		Schema schema = new Schema("testSchema", Fields.parse("a:string, b:int, c:double, d:float, e:boolean, f:long"));
		ITuple tuple = new Tuple(schema);
		tuple.set(0, "foo");
		tuple.set(1, 10);
		tuple.set(2, 20d);
		tuple.set(3, 30f);
		tuple.set(4, false);
		tuple.set(5, 40l);
		
		NullableTuple nullableTuple = new NullableTuple(tuple);
		assertEquals("foo", nullableTuple.get(0).toString());
		assertEquals(10, nullableTuple.get(1));
		assertEquals(20d, nullableTuple.get(2));
		assertEquals(30f, nullableTuple.get(3));
		assertEquals(false, nullableTuple.get(4));
		assertEquals(40l, nullableTuple.get(5));
	}
	
	@Test
	public void testNoNullableWrapperWithNulls() {
		Schema schema = new Schema("testSchema", Fields.parse("a:string, b:int, c:double, d:float, e:boolean, f:long"));
		ITuple tuple = new Tuple(schema);
		tuple.set(0, "foo");
		tuple.set(1, null);
		tuple.set(2, 20d);
		tuple.set(3, null);
		tuple.set(4, false);
		tuple.set(5, null);
		
		NullableTuple nullableTuple = new NullableTuple(tuple);
		assertEquals("foo", nullableTuple.getNullable(0).toString());
		assertEquals(null, nullableTuple.getNullable(1));
		assertEquals(20d, nullableTuple.getNullable(2));
		assertEquals(null, nullableTuple.getNullable(3));
		assertEquals(false, nullableTuple.getNullable(4));
		assertEquals(null, nullableTuple.getNullable(5));
	}
	
	@Test
	public void testDirectUseNoWrapper() {
		Schema schema = new Schema("testSchema", Fields.parse("a:string, b:int, c:double"));
		NullableTuple nullableTuple = new NullableTuple(schema);
		nullableTuple.set(0, "foo");
		nullableTuple.set("b", null);
		nullableTuple.set("c", 20d);
		
		assertEquals("foo", nullableTuple.getNullable("a").toString());
		assertEquals(null, nullableTuple.getNullable(1));
		assertEquals(20d, nullableTuple.getNullable(2));
	}
	
	@Test
	public void testNullableOfNullable() {
		Schema schema = new Schema("testSchema", Fields.parse("a:string, b:int, c:double"));
		NullableTuple nullableTuple = new NullableTuple(schema);
		nullableTuple.set(0, "foo");
		nullableTuple.set("b", null);
		nullableTuple.set("c", 20d);

		NullableTuple tuple2 = new NullableTuple(nullableTuple);
		assertEquals("foo", tuple2.getNullable("a").toString());
		assertEquals(null, tuple2.getNullable(1));
		assertEquals(20d, tuple2.getNullable(2));
		assertEquals(nullableTuple.getSchema().getFields().size(), tuple2.getSchema().getFields().size());
	}
	
	@Test
	public void testModifying() {
		Schema schema = new Schema("testSchema", Fields.parse("a:string, b:int, c:double, d:float, e:boolean, f:long"));
		ITuple tuple = new Tuple(schema);
		tuple.set(0, "foo");
		tuple.set(1, null);
		tuple.set(2, 20d);
		tuple.set(3, null);
		tuple.set(4, false);
		tuple.set(5, null);
		
		NullableTuple nullableTuple = new NullableTuple(tuple);
		assertEquals("foo", nullableTuple.getNullable(0).toString());
		assertEquals(null, nullableTuple.getNullable(1));
		assertEquals(20d, nullableTuple.getNullable(2));
		assertEquals(null, nullableTuple.getNullable(3));
		assertEquals(false, nullableTuple.getNullable(4));
		assertEquals(null, nullableTuple.getNullable(5));
		
		nullableTuple.set(2, null);
		nullableTuple.set(4, null);
		nullableTuple.set(0, null);
		nullableTuple.set(1, 10);
		nullableTuple.set(3, 20f);
		nullableTuple.set(5, 30l);
		
		assertEquals(null, nullableTuple.getNullable(0));
		assertEquals(10, nullableTuple.getNullable(1));
		assertEquals(null, nullableTuple.getNullable(2));
		assertEquals(20f, nullableTuple.getNullable(3));
		assertEquals(null, nullableTuple.getNullable(4));
		assertEquals(30l, nullableTuple.getNullable(5));
	}
	
	@Test
	public void testMoreThanEightProps() {
		List<Field> fields = new ArrayList<Field>(30);
		for(int i = 0; i < 30; i++) {
			fields.add(Field.create("field" + i, Type.STRING));
		}
		Schema schema = new Schema("bigmammaschema", fields);
		
		ITuple tuple = new Tuple(schema);
		for(int i = 0; i < 30; i++) {
			// Fill in with some default values
			tuple.set("field" + i, "defaultValue" + i);
		}
		
		NullableTuple nullableTuple = new NullableTuple(tuple);
		for(int i = 0; i < 30; i++) {
			// Assert the default values
			assertEquals("defaultValue" + i, nullableTuple.get("field" + i));
		}
		
		// Set fields to null one by one and assert that things go well
		for(int i = 0; i < 30; i++) {
			nullableTuple.set("field" + i, null);
			assertEquals(null, nullableTuple.getNullable("field" + i));
			nullableTuple.set("field" + i, "defaultValue" + i);
			assertEquals("defaultValue" + i, nullableTuple.getNullable("field" + i));
		}
	}
}
