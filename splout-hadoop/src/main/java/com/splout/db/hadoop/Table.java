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

import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;

/**
 * Immutable definition of a Splout Table whose view has to be generated. It can be obtained throuh {@link TableBuilder}.
 */
public class Table {

  private final ImmutableList<TableInput> files;
  private final TableSpec tableSpec;

  Table(TableInput inputFile, TableSpec tableSpec) {
    this(Arrays.asList(new TableInput[]{inputFile}), tableSpec);
  }

  Table(List<TableInput> files, TableSpec tableSpec) {
    this.files = ImmutableList.copyOf(files);
    this.tableSpec = tableSpec;
  }

  // ---- Getters ---- //

  public List<TableInput> getFiles() {
    return files;
  }

  public TableSpec getTableSpec() {
    return tableSpec;
  }
}
