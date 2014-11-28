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

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.MapContext;

/**
 * A wrapper around a common functionality used in Hadoop: using Counters. This wrapper is provided in {@link RecordProcessor}.
 * We wrap certain functionality to be able to easily mock the Hadoop context, for example in sampling where we don't have a real
 * Hadoop Context but we still don't want the user to care about NPE's in the {@link RecordProcessor}.
 */
@SuppressWarnings("rawtypes")
public class CounterInterface {

  private MapContext context;

  CounterInterface(MapContext context) {
    this.context = context;
  }

  public Counter getCounter(String group, String name) {
    return context.getCounter(group, name);
  }
}
