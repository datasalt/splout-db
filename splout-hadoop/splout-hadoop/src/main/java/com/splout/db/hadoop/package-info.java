/**
 * This package contains all the classes that implement business logic for helping building 
 * Splout datastores using Hadoop with Pangool (<a href='http://pangool.net'>http://pangool.net</a>).
 * <p>
 * Here we have the low-level OutputFormat {@link com.splout.db.hadoop.SQLiteOutputFormat} that can be used to build
 * any partitioned SQL from any Hadoop Job. Then we have the {@link com.splout.db.hadoop.TupleSQLite4JavaOutputFormat} that
 * is built on top of the previous and that is more efficient and easy to use when used in conjunction
 * with Pangool.
 * <p>
 * Finally we have a full Hadoop process that builds a partitioned store from a Pangool Tuple file set, {@link com.splout.db.hadoop.TablespaceGenerator}.
 * We need to use {@link com.splout.db.hadoop.TableBuilder} and {@link com.splout.db.hadoop.TablespaceBuilder} for providing the needed information to it.
 */
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
