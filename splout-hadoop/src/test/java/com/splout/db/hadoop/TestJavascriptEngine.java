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

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestJavascriptEngine {

  @Test
  public void test() throws Throwable {

    Map<String, Object> record = new HashMap<String, Object>();
    record.put("a", "A");

    JavascriptEngine engine = new JavascriptEngine("function run(record) { return record.get('a'); }");
    assertEquals("A", engine.execute("run", record));

    record.put("a", "B");

    engine = new JavascriptEngine("function run(record) { return record.get('a'); }");
    assertEquals("B", engine.execute("run", record));
  }
}
