package com.splout.db.common;

/*
 * #%L
 * Splout SQL commons
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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.splout.db.engine.ResultSerializer;
import com.splout.db.engine.ResultSerializer.SerializationException;

public class TestSerialization {

  @Test
  public void testKryo() throws SerializationException {
    String[] columnNames = new String[] { "foo", "int_prop" };
    List<Object[]> results = new ArrayList<Object[]>();
    Object[] result = new Object[] {  "bar", 0 };
    results.add(result);

    ByteBuffer serialized = ResultSerializer.serialize(new QueryResult(columnNames, results));

    QueryResult read = ResultSerializer.deserialize(serialized);
    
    assertEquals("bar", read.getResults().get(0)[0]);
    assertEquals(0, read.getResults().get(0)[1]);
    
    assertEquals("foo", read.getColumnNames()[0]);
    assertEquals("int_prop", read.getColumnNames()[1]);
  }
}
