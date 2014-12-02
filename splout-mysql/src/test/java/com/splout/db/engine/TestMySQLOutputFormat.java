package com.splout.db.engine;

/*
 * #%L
 * Splout MySQL
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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.List;

import org.junit.Test;

import com.splout.db.hadoop.engine.SploutSQLOutputFormatTester;

@SuppressWarnings("serial")
public class TestMySQLOutputFormat extends SploutSQLOutputFormatTester {

	@SuppressWarnings("rawtypes")
	@Test
	public void test() throws Exception {
		Runtime.getRuntime().exec("rm -rf " + OUTPUT + "-mysql").waitFor();
		
		getTupleSchema1().getField("a").addProp(MySQLOutputFormat.STRING_FIELD_SIZE_PANGOOL_FIELD_PROP, "8");
		
		runTest(new MySQLEngine());

		File dbFile = new File(OUTPUT + "/0.db");
				
		MySQLManager manager = new MySQLManager();
		try {
			manager.init(dbFile, null, null);
			List list = manager.query("SELECT * FROM schema1;", 100).mapify();
			assertEquals(6, list.size());
		} finally {
			manager.close();
		}
	}
}
