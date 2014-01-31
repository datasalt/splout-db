package com.splout.db.engine;

/*
 * #%L
 * Splout Redis
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.splout.db.common.JSONSerDe;
import com.splout.db.hadoop.engine.SploutSQLOutputFormatTester;

@SuppressWarnings({ "serial", "unchecked", "rawtypes" })
public class TestRedisOutputFormat extends SploutSQLOutputFormatTester {

	@Test
	public void test() throws Exception {
		runTest(new RedisEngine());
		
		RedisManager manager = new RedisManager();
		manager.init(new File(OUTPUT, "0.db"), BaseTest.getBaseConfiguration(), null);
		
		List l = JSONSerDe.deSer(manager.query("foo1", 1), ArrayList.class);
    Map<String, Object> record = (Map<String, Object>)l.get(0);
		
		assertEquals("foo1", record.get("a"));
		assertEquals(30, record.get("b"));
		
		manager.close();
	}
}
