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

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleReducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * Reusable TupleReducer that aggregates the counts of a certain field in the Tuple.
 * <p/>
 * There are nicer implementations of this in the work-in-progress Pangool-flow project of Pangool but I decided not to
 * depend on it until it has a first stable version.
 */
@SuppressWarnings("serial")
public class SumReducer extends TupleReducer<ITuple, NullWritable> {

  String field;

  /**
   * Will (int) sum the field "field"
   */
  public SumReducer(String field) {
    this.field = field;
  }

  public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector)
      throws IOException, InterruptedException, TupleMRException {
    int totalCount = 0;
    ITuple outTuple = null;
    for (ITuple tuple : tuples) {
      totalCount += (Integer) tuple.get(field);
      outTuple = tuple;
    }
    outTuple.set(field, totalCount);
    collector.write(outTuple, NullWritable.get());
  }
}
