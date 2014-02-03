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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.splout.db.common.JSONSerDe;
import com.splout.db.engine.DefaultEngine;
import com.splout.db.engine.SQLite4JavaClient;
import com.splout.db.hadoop.TableSpec;
import com.splout.db.hadoop.TableSpec.FieldIndex;

@SuppressWarnings("serial")
public class TestSQLite4JavaOutputFormat extends SploutSQLOutputFormatTester implements Serializable {

	@Test
	public void testCompoundIndexes() throws Exception {
		final Schema tupleSchema1 = new Schema("schema1", Fields.parse("a:string, b:int"));
		TableSpec tableSpec = new TableSpec(tupleSchema1, new Field[] { tupleSchema1.getField(0) },
		    new FieldIndex[] { new FieldIndex(tupleSchema1.getField(0), tupleSchema1.getField(1)) }, null,
		    null, null, null, null);
		String[] createIndex = SploutSQLOutputFormat.getCreateIndexes(tableSpec);
		assertEquals("CREATE INDEX idx_schema1_ab ON schema1(`a`, `b`);", createIndex[0]);
	}

	@Test
	public void test() throws Exception {
		runTest(new DefaultEngine());
		
		// Assert that the DB has been created successfully
		
		assertTrue(new File(OUTPUT + "/0.db").exists());
		SQLite4JavaClient manager = new SQLite4JavaClient(OUTPUT + "/0.db", null);
		@SuppressWarnings("rawtypes")
    List list = JSONSerDe.deSer(manager.query("SELECT * FROM schema1;", 100), ArrayList.class);
		assertEquals(6, list.size());
		
		manager.close();
	}
}
