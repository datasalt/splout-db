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

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.splout.db.common.HeartBeater;

/**
 * Low-level Pangool OutputFormat that can be used to generate partitioned SQL views in Hadoop. It accepts Tuples that
 * have "sql" strings and "partition" integers. Each partition will generate a different .db file named <partition>.db
 * <p>
 * Furthermore, the OutputFormat accepts a list of initial SQL statements that will be executed for each partition when
 * the database is created (e.g. CREATE TABLE and such). It also accepts finalization statements (e.g. CREATE INDEX).
 * <p>
 * This OutputFormat can be used as a basis for creating more complex OutputFormats such as
 * {@link TupleSQLiteOutputFormat}.
 * <p>
 * Moreover, using this OutputFormat directly can result in poor-performing Jobs as it can't cache PreparedStatements
 * (it has to create a new Statement for every SQL it receives).
 */
@SuppressWarnings("serial")
public class SQLiteOutputFormat extends FileOutputFormat<ITuple, NullWritable> implements Serializable {
	public static Log LOG = LogFactory.getLog(SQLiteOutputFormat.class);

	// The Pangool Tuple Schema that will be received as output: SQL and partition number to write to
	public final static Schema SCHEMA = new Schema("sql", Fields.parse("sql:string, partition:int"));

	private static AtomicLong FILE_SEQUENCE = new AtomicLong(0);

	// Number of SQL inserts that will be performed before committing a transaction.
	// Recommended around 1000000 for performance.
	private int batchSize;
	
	List<String> initSqlStatements = null;
	List<String> endSqlStatements = null;

	public SQLiteOutputFormat(String[] initSqlStatements, String[] endSqlStatements, int batchSize) {
		if(initSqlStatements != null) {
			this.initSqlStatements = Arrays.asList(initSqlStatements);
		}
		if(endSqlStatements != null) {
			this.endSqlStatements = Arrays.asList(endSqlStatements);
		}
		this.batchSize = batchSize;
	}

	/*
	 * The main complexity here resides in maintaining separate transactions for each partition. We use transactions for
	 * efficiency (bulk inserting). For that purpose we cache the Statement, Connection and number of executed SQL actions
	 * for each partition (statementPool, connectionPool, executedMap).
	 */
	public class SQLRecordWriter extends RecordWriter<ITuple, NullWritable> {

		// Temporary and permanent Paths for properly writing Hadoop output files
		private Map<Integer, Path> permPool = new HashMap<Integer, Path>();
		private Map<Integer, Path> tempPool = new HashMap<Integer, Path>();

		// Transaction cache
		private Map<Integer, Statement> statementPool = new HashMap<Integer, Statement>();
		private Map<Integer, Connection> connectionPool = new HashMap<Integer, Connection>();
		private Map<Integer, Integer> executedMap = new HashMap<Integer, Integer>();

		private Configuration conf;
		private HeartBeater heartBeater;
		private TaskAttemptContext context;
		private FileSystem fs;

		public SQLRecordWriter(TaskAttemptContext context) throws IOException {
			this.context = context;
			heartBeater = new HeartBeater(context);
			heartBeater.needHeartBeat();
			conf = context.getConfiguration();
		}

		// This method is called one time per each partition
		protected void initSql(int partition) throws IOException {
			Path outPath = FileOutputFormat.getOutputPath(context);
			fs = outPath.getFileSystem(conf);
			Path perm = new Path(FileOutputFormat.getOutputPath(context), partition + ".db");
			Path temp = conf.getLocalPath("mapred.local.dir", partition + "." + FILE_SEQUENCE.incrementAndGet());
			fs.delete(perm, true); // delete old, if any
			fs.delete(temp, true); // delete old, if any
			Path local = fs.startLocalOutput(perm, temp);
			//
			try {
				permPool.put(partition, perm);
				tempPool.put(partition, temp);
				// Load the sqlite-JDBC driver using the current class loader
				LOG.info("Initializing SQL connection [" + partition + "]");
				Class.forName("org.sqlite.JDBC");
				// Get a Connection
				Connection conn = DriverManager.getConnection("jdbc:sqlite:" + local.toString());
				connectionPool.put(partition, conn);
				// Create a Statement
				Statement st = conn.createStatement();
				statementPool.put(partition, st);
				// Execute initial statements
				if(initSqlStatements != null) {
					LOG.info("Executing initial SQL statements.");
					for(String sql : initSqlStatements) {
						LOG.info("Executing: " + sql);
						st.execute(sql);
					}
				}
				// PRAGMA optimizations as per http://www.sqlite.org/pragma.html#pragma_count_changes
				st.execute("PRAGMA synchronous=OFF");
				st.execute("PRAGMA journal_mode=OFF");
				st.execute("PRAGMA temp_store=FILE");
				// Init transaction
				st.execute("BEGIN");
			} catch(SQLException e) {
				throw new IOException(e);
			} catch(ClassNotFoundException e) {
				throw new IOException(e);
			}
		}

		// Subclasses may need access to the connection pool
		protected Map<Integer, Connection> getConnectionPool() {
			return connectionPool;
		}

		// Subclasses may need access to the statement pool
		public Map<Integer, Statement> getStatementPool() {
    	return statementPool;
    }

		// Finalize: finish pending transactions, execute end statements
		@Override
		public void close(TaskAttemptContext ctx) throws IOException, InterruptedException {
			try {
				if(ctx != null) {
					heartBeater.setProgress(ctx);
				}
				for(Map.Entry<Integer, Statement> entry : statementPool.entrySet()) {
					LOG.info("Closing SQL connection [" + entry.getKey() + "]");
					Statement st = entry.getValue();
					st.execute("COMMIT");
					st.close();
					if(endSqlStatements != null) {
						LOG.info("Executing end SQL statements.");
						for(String sql : endSqlStatements) {
							LOG.info("Executing: " + sql);
							st.execute(sql);
						}
					}
					// Hadoop - completeLocalOutput()
					fs.completeLocalOutput(permPool.get(entry.getKey()), tempPool.get(entry.getKey()));
				}
			} catch(SQLException e) {
				throw new IOException(e);
			} finally { // in any case, destroy the HeartBeater
				heartBeater.cancelHeartBeat();
			}
		}

		@Override
		public void write(ITuple tuple, NullWritable ignore) throws IOException, InterruptedException {
			String sql = tuple.get("sql").toString();

			int partition = (Integer) tuple.get("partition");
			try {
				if(statementPool.get(partition) == null) {
					// Execute warm-up for this partition
					initSql(partition);
				}
				Statement st = statementPool.get(partition);
				// Execute the SQL
				st.execute(sql);
				Integer exec = MapUtils.getInteger(executedMap, partition, 0);
				// Increment the number of statements executed for this partition
				exec++;
				if(exec % batchSize == 0) {
					// Close transaction and begin another one if > BATCH_SIZE
					st.execute("COMMIT");
					st.execute("BEGIN");
				}
				executedMap.put(partition, exec);
			} catch(SQLException e) {
				throw new IOException(e);
			} finally {
				heartBeater.cancelHeartBeat();
			}
		}
	}

	@Override
	public RecordWriter<ITuple, NullWritable> getRecordWriter(final TaskAttemptContext context) throws IOException,
	    InterruptedException {
		return new SQLRecordWriter(context);
	}
}