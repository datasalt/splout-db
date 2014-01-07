package com.splout.db.hadoop.engine;

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

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema.Field;
import com.splout.db.hadoop.TableSpec;

/**
 * An OutputFormat that accepts Pangool's Tuples and writes to a sqlite4Java SQLite file. The Tuples that are written to
 * it must conform to a particular schema: having a "_partition" integer field (which will then create a file named
 * "partition".db).
 * <p>
 * The different schemas that will be given to this OutputFormat are defined in the constructor by providing a
 * {@link TableSpec}. These TableSpec also contains information such as pre-SQL or post-SQL statements but most notably
 * contain a Schema so that a CREATE TABLE can be derived automatically from it. Note that the Schema provided to
 * TableSpec doesn't need to contain a "_partition" field or be nullable.
 */
@SuppressWarnings("serial")
public class SQLite4JavaOutputFormat extends SploutSQLOutputFormat implements Serializable {

	public static Log LOG = LogFactory.getLog(SQLite4JavaOutputFormat.class);

	public SQLite4JavaOutputFormat(Integer batchSize, TableSpec... dbSpecs)
	    throws SploutSQLOutputFormatException {
		super(batchSize, dbSpecs);
	}

	// Given a {@link TableSpec}, returns the appropriated SQL CREATE TABLE...
	public String getCreateTable(TableSpec tableSpec) throws SploutSQLOutputFormatException {
		String createTable = "CREATE TABLE " + tableSpec.getSchema().getName() + " (";
		for(Field field : tableSpec.getSchema().getFields()) {
			if(field.getName().equals(PARTITION_TUPLE_FIELD)) {
				continue;
			}
			createTable += field.getName() + " ";
			switch(field.getType()) {
			/*
			 * This mapping is done after SQLite's documentation. For instance, SQLite doesn't have Booleans (have to be
			 * INTEGERs). It doesn't have LONGS either.
			 */
			case INT:
				createTable += "INTEGER, ";
				break;
			case LONG:
				createTable += "INTEGER, ";
				break;
			case DOUBLE:
				createTable += "REAL, ";
				break;
			case FLOAT:
				createTable += "REAL, ";
				break;
			case STRING:
				createTable += "TEXT, ";
				break;
			case BOOLEAN:
				createTable += "INTEGER, ";
				break;
			default:
				throw new SploutSQLOutputFormatException("Unsupported field type: " + field.getType());
			}
		}
		createTable = createTable.substring(0, createTable.length() - 2);
		return createTable += ");";
	}

	// Map of prepared statements per Schema and per Partition
	private Map<Integer, Map<String, SQLiteStatement>> stCache = new HashMap<Integer, Map<String, SQLiteStatement>>();
	private Map<Integer, SQLiteConnection> connCache = new HashMap<Integer, SQLiteConnection>();

	private long records = 0;

	// This method is called one time per each partition
	public void initPartition(int partition, Path local) throws IOException, InterruptedException {
		try {
			LOG.info("Initializing SQL connection [" + partition + "]");
			SQLiteConnection conn = new SQLiteConnection(new File(local.toString()));
			// Change the default temp_store_directory, otherwise we may run out of disk space as it will go to /var/tmp
			// In EMR the big disks are at /mnt
			// It suffices to set it to . as it is the tasks' work directory
			// Warning: this pragma is deprecated and may be removed in further versions, however there is no choice
			// other than recompiling SQLite or modifying the environment.
			conn.open(true);
			conn.exec("PRAGMA temp_store_directory = '" + new File(".").getAbsolutePath() + "'");
			SQLiteStatement st = conn.prepare("PRAGMA temp_store_directory");
			st.step();
			LOG.info("Changed temp_store_directory to: " + st.columnString(0));
			// journal_mode=OFF speeds up insertions
			conn.exec("PRAGMA journal_mode=OFF");
			/*
			 * page_size is one of of the most important parameters for speed up indexation. SQLite performs a merge sort for
			 * sorting data before inserting it in an index. The buffer SQLites uses for sorting has a size equals to
			 * page_size * SQLITE_DEFAULT_TEMP_CACHE_SIZE. Unfortunately, SQLITE_DEFAULT_TEMP_CACHE_SIZE is a compilation
			 * parameter. That is then fixed to the sqlite4java library used. We have recompiled that library to increase
			 * SQLITE_DEFAULT_TEMP_CACHE_SIZE (up to 32000 at the point of writing this lines), so, at runtime the unique way
			 * to change the buffer size used for sorting is change the page_size. page_size must be changed BEFORE CREATE
			 * STATEMENTS, otherwise it won't have effect. page_size should be a multiple of the sector size (1024 on linux)
			 * in order to be efficient.
			 */
			conn.exec("PRAGMA page_size=8192;");
			connCache.put(partition, conn);
			// Init transaction
			for(String sql : getPreSQL()) {
				LOG.info("Executing: " + sql);
				conn.exec(sql);
			}
			conn.exec("BEGIN");
			Map<String, SQLiteStatement> stMap = new HashMap<String, SQLiteStatement>();
			stCache.put(partition, stMap);
		} catch(SQLiteException e) {
			throw new IOException(e);
		} catch(SploutSQLOutputFormatException e) {
	    throw new IOException(e);
    }
	}

	@Override
	public void write(ITuple tuple) throws IOException, InterruptedException {
		int partition = (Integer) tuple.get(PARTITION_TUPLE_FIELD);

		try {
			/*
			 * Key performance trick: Cache PreparedStatements when possible. We will have one PreparedStatement per each
			 * different Tuple Schema (table).
			 */
			Map<String, SQLiteStatement> stMap = stCache.get(partition);

			SQLiteStatement pS = stMap.get(tuple.getSchema().getName());
			if(pS == null) {
				SQLiteConnection conn = connCache.get(partition);
				// Create a PreparedStatement according to the received Tuple
				String preparedStatement = "INSERT INTO " + tuple.getSchema().getName() + " VALUES (";
				// NOTE: tuple.getSchema().getFields().size() - 1 : quick way of skipping "_partition" fields here
				for(int i = 0; i < tuple.getSchema().getFields().size() - 1; i++) {
					preparedStatement += "?, ";
				}
				preparedStatement = preparedStatement.substring(0, preparedStatement.length() - 2) + ");";
				pS = conn.prepare(preparedStatement);
				stMap.put(tuple.getSchema().getName(), pS);
			}

			int count = 1, tupleCount = 0;
			for(Field field : tuple.getSchema().getFields()) {
				if(field.getName().equals(PARTITION_TUPLE_FIELD)) {
					tupleCount++;
					continue;
				}

				if(tuple.get(tupleCount) == null) {
					pS.bindNull(count);
				} else {
					switch(field.getType()) {

					case INT:
						pS.bind(count, (Integer) tuple.get(tupleCount));
						break;
					case LONG:
						pS.bind(count, (Long) tuple.get(tupleCount));
						break;
					case DOUBLE:
						pS.bind(count, (Double) tuple.get(tupleCount));
						break;
					case FLOAT:
						pS.bind(count, (Float) tuple.get(tupleCount));
						break;
					case STRING:
						pS.bind(count, tuple.get(tupleCount).toString());
						break;
					case BOOLEAN: // Remember: In SQLite there are no booleans
						pS.bind(count, ((Boolean) tuple.get(tupleCount)) == true ? 1 : 0);
					default:
						break;
					}
				}
				count++;
				tupleCount++;
			}
			pS.step();
			pS.reset();

			records++;
			if(records == getBatchSize()) {
				SQLiteConnection conn = connCache.get(partition);
				conn.exec("COMMIT");
				conn.exec("BEGIN");
				records = 0;
			}
		} catch(SQLiteException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void close() throws IOException, InterruptedException {
		try {
			for(Map.Entry<Integer, SQLiteConnection> entry : connCache.entrySet()) {
				LOG.info("Closing SQL connection [" + entry.getKey() + "]");
				//
				entry.getValue().exec("COMMIT");
				if(getPostSQL() != null) {
					LOG.info("Executing end SQL statements.");
					for(String sql : getPostSQL()) {
						LOG.info("Executing: " + sql);
						entry.getValue().exec(sql);
					}
				}
				entry.getValue().dispose();
			}
		} catch(SQLiteException e) {
			throw new IOException(e);
		} catch(SploutSQLOutputFormatException e) {
			throw new IOException(e);
    }
	}
}
