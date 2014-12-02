package com.splout.db.common;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.splout.db.engine.ResultAndCursorId;
import com.splout.db.engine.ResultSerializer;
import com.splout.db.engine.ResultSerializer.SerializationException;

public class TestSerialization {

  @Test
  public void testKryo() throws SerializationException {
    String[] columnNames = new String[] { "foo", "int_prop" };
    List<Object[]> results = new ArrayList<Object[]>();
    Object[] result = new Object[] {  "bar", 0 };
    results.add(result);

    ByteBuffer serialized = ResultSerializer.serialize(new ResultAndCursorId(new QueryResult(columnNames, results), 1));

    ResultAndCursorId read = ResultSerializer.deserialize(serialized);
    
    assertEquals("bar", read.getResult().getResults().get(0)[0]);
    assertEquals(0, read.getResult().getResults().get(0)[1]);
    
    assertEquals("foo", read.getResult().getColumnNames()[0]);
    assertEquals("int_prop", read.getResult().getColumnNames()[1]);
    
    assertEquals(1, read.getCursorId());
  }
}
