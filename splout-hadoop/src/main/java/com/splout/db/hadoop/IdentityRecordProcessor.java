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

/**
 * This is the default {@link RecordProcessor} that just bypasses the Tuples comming in from the input files. Thus, it is expected
 * that the Schema in the {@link TableInput} files matches the Schema of the Table that will be produced by the {@link TablespaceGenerator}.
 */
@SuppressWarnings("serial")
public class IdentityRecordProcessor extends RecordProcessor {

  @Override
  public ITuple process(ITuple record, String tableName, CounterInterface context) throws Throwable {
    return record;
  }
}
