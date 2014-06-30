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

import com.splout.db.common.PartitionEntry;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

@SuppressWarnings({"rawtypes", "serial"})
public class TestTablespaceGeneratorGeneratePartitions {

  public static class ReaderMockup implements TablespaceGenerator.Nextable {

    Iterator<String> it;

    public ReaderMockup(String[] sampleKeysSorted) throws IOException {
      it = Arrays.asList(sampleKeysSorted).iterator();
    }

    @Override
    public synchronized boolean next(Writable key) throws IOException {
      boolean ret = it.hasNext();
      if (ret) {
        ((Text) key).set(it.next());
      }
      return ret;
    }
  }

  private void testCase(String[] sampleKeysSorted, int nPartitions, String[] expectedPartitionMap) throws IOException {
    ReaderMockup reader = new ReaderMockup(sampleKeysSorted);
    List<PartitionEntry> partitionEntries = TablespaceGenerator.calculatePartitions(nPartitions, sampleKeysSorted.length, reader);

    System.out.println("Expected: " + toS(expectedPartitionMap) + ", Obtained: " + toS(partitionEntries));
    assertEquals(expectedPartitionMap.length - 1, partitionEntries.size());
    for (int i = 0; i < expectedPartitionMap.length - 2; i++) {
      PartitionEntry pe = partitionEntries.get(i);
      assertEquals(expectedPartitionMap[i], pe.getMin());
      assertEquals(expectedPartitionMap[i + 1], pe.getMax());
      assertEquals(i, (int) pe.getShard());
    }
  }

  private String toS(String[] keys) {
    String ret = "";
    for (int i = 0; i < keys.length; i++) {
      if (i != 0) {
        ret += ",";
      }
      ret += keys[i];
    }
    return "(" + ret + ")";
  }

  private String toS(List<PartitionEntry> keys) {
    String ret = "";
    for (int i = 0; i < keys.size(); i++) {
      if (i != 0) {
        ret += ",";
      } else {
        ret += keys.get(i).getMin() + ",";
      }
      ret += keys.get(i).getMax();
    }
    return "(" + ret + ")";
  }

  private static String[] s(String... keys) {
    return keys;
  }

  @Test
  public void test() throws Exception {
    testCase(s("A"), 1, s(null, null));
    testCase(s("A"), 2, s(null, null));
    testCase(s("A"), 10, s(null, null));

    testCase(s("A", "B"), 1, s(null, null));
    testCase(s("A", "B"), 2, s(null, "A", null));
    testCase(s("A", "B"), 3, s(null, "A", null));
    testCase(s("A", "B"), 4, s(null, "A", null));

    testCase(s("A", "A", "A"), 1, s(null, null));
    testCase(s("A", "A", "A"), 2, s(null, null));
    testCase(s("A", "A", "A"), 3, s(null, null));
    testCase(s("A", "A", "A"), 10, s(null, null));
    testCase(s("A", "A", "B"), 3, s(null, "A", null));

    testCase(s("A", "A", "A", "B", "B"), 1, s(null, null));
    testCase(s("A", "A", "A", "A", "B"), 2, s(null, "A", null));
    testCase(s("A", "B", "B", "B", "B"), 2, s(null, "A", null));
    testCase(s("A", "A", "A", "A", "B"), 10, s(null, "A", null));
    testCase(s("A", "B", "B", "B", "B"), 10, s(null, "A", null));

    testCase(s("A", "B", "C", "D", "E"), 2, s(null, "B", null));
    testCase(s("A", "B", "C", "D", "E"), 3, s(null, "A", "B", null));
    testCase(s("A", "B", "C", "D", "E"), 5, s(null, "A", "B", "C", "D", null));
    testCase(s("A", "B", "C", "D", "E"), 6, s(null, "A", "B", "C", "D", null));

    testCase(s("A", "B", "C", "D", "E", "F", "G", "H", "I"), 3, s(null, "C", "F", null));
    testCase(s("A", "B", "C", "F", "F", "F", "G", "H", "I"), 3, s(null, "C", "F", null));
    testCase(s("A", "B", "F", "F", "F", "F", "G", "H", "I"), 3, s(null, "F", "G", null));
    testCase(s("A", "B", "C", "F", "F", "F", "F", "H", "I"), 3, s(null, "C", "F", null));
    testCase(s("A", "B", "C", "F", "F", "F", "F", "H", "I"), 4, s(null, "B", "F", "H", null));
    testCase(s("A", "B", "C", "F", "F", "F", "F", "H", "H"), 4, s(null, "B", "F", null));
    testCase(s("A", "B", "C", "F", "F", "F", "G", "H", "H"), 4, s(null, "B", "F", "G", null));
  }
}