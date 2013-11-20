package com.splout.db.hadoop.engine;

/*
 * #%L
 * Splout SQL Hadoop library
 * %%
 * Copyright (C) 2012 - 2013 Datasalt Systems S.L.
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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.splout.db.common.CompressorUtil;
import com.splout.db.common.HeartBeater;
import com.splout.db.engine.EmbeddedMySQL;
import com.splout.db.engine.EmbeddedMySQL.EmbeddedMySQLConfig;
import com.splout.db.engine.MySQLManager;
import com.splout.db.hadoop.TableSpec;

@SuppressWarnings("serial")
public class MySQLOutputFormat extends SploutSQLOutputFormat {

	public static Log LOG = LogFactory.getLog(MySQLOutputFormat.class);

	public static String STRING_FIELD_SIZE_PROP = "com.splout.db.hadoop.engine.MySQLOutputFormat.string.field.size";
	public static String GENERATED_DB_NAME = "splout";

	// Keep track of all opened mysqlds so we can kill them in any case
	private Map<Integer, EmbeddedMySQL> mySQLs = new HashMap<Integer, EmbeddedMySQL>();

	public MySQLOutputFormat(int batchSize, TableSpec... dbSpec) throws SploutSQLOutputFormatException {
		super(batchSize, dbSpec);
	}

	@Override
	public String getCreateTable(TableSpec tableSpec) throws SploutSQLOutputFormatException {
		String createTable = "CREATE TABLE " + tableSpec.getSchema().getName() + " (";
		for(Field field : tableSpec.getSchema().getFields()) {
			int fieldSize = -1;
			if(field.getProp(STRING_FIELD_SIZE_PROP) != null) {
				fieldSize = Integer.parseInt(field.getProp(STRING_FIELD_SIZE_PROP));
			}
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
				createTable += "LONG, ";
				break;
			case DOUBLE:
				createTable += "DOUBLE, ";
				break;
			case FLOAT:
				createTable += "FLOAT, ";
				break;
			case STRING:
				if(fieldSize > 0) {
					createTable += "VARCHAR(" + fieldSize + "), ";
				} else {
					createTable += "TEXT, ";
				}
				break;
			case BOOLEAN:
				createTable += "BOOLEAN, ";
				break;
			default:
				throw new SploutSQLOutputFormatException("Unsupported field type: " + field.getType());
			}
		}
		createTable = createTable.substring(0, createTable.length() - 2);
		return createTable += ") ENGINE=InnoDB DEFAULT CHARSET=UTF8;";
	}

	@Override
	public RecordWriter<ITuple, NullWritable> getRecordWriter(TaskAttemptContext context)
	    throws IOException, InterruptedException {

		return new TupleSQLRecordWriter(context);
	}

	private static AtomicLong FILE_SEQUENCE = new AtomicLong(0);

	/**
	 * A RecordWriter that accepts an Int(Partition), a Tuple and delegates to a {@link SQLRecordWriter} converting the
	 * Tuple into SQL and assigning the partition that comes in the Key.
	 */
	public class TupleSQLRecordWriter extends RecordWriter<ITuple, NullWritable> {

		// Temporary and permanent Paths for properly writing Hadoop output files
		private Map<Integer, Path> permPool = new HashMap<Integer, Path>();
		private Map<Integer, Path> tempPool = new HashMap<Integer, Path>();

		// Map of prepared statements per Schema and per Partition
		private Map<Integer, Map<String, PreparedStatement>> stCache = new HashMap<Integer, Map<String, PreparedStatement>>();
		private Map<Integer, Connection> connCache = new HashMap<Integer, Connection>();

		private HeartBeater heartBeater;

		long records = 0;
		private FileSystem fs;
		private Configuration conf;
		private TaskAttemptContext context;

		public TupleSQLRecordWriter(TaskAttemptContext context) {
			this.context = context;
			long waitTimeHeartBeater = context.getConfiguration().getLong(HeartBeater.WAIT_TIME_CONF, 5000);
			heartBeater = new HeartBeater(context, waitTimeHeartBeater);
			heartBeater.needHeartBeat();
			conf = context.getConfiguration();
		}

		// This method is called one time per each partition
		private void initSql(int partition) throws IOException {

			// HDFS final location of the generated MySQL files. It will be
			// loaded to the temporary folder in the HDFS than finally will be
			// committed by the OutputCommitter to the proper location.
			FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(context);
			Path perm = new Path(committer.getWorkPath(), partition + ".db");
			fs = perm.getFileSystem(context.getConfiguration());

			// Make a task unique name that contains the actual index output name to
			// make debugging simpler
			// Note: if using JVM reuse, the sequence number will not be reset for a
			// new task using the jvm
			Path temp = conf.getLocalPath("mapred.local.dir", "splout_task_" + context.getTaskAttemptID()
			    + '.' + FILE_SEQUENCE.incrementAndGet());

			// Final "partition".db file that will be uploaded to HDFS
			fs.startLocalOutput(perm, new Path(temp, partition + ".db"));
			// Local folder where MySQL will be instantiated
			Path mysqlDb = fs.startLocalOutput(new Path(committer.getWorkPath(), partition + ""), new Path(
			    temp, partition + ""));

			//
			try {
				permPool.put(partition, perm);
				tempPool.put(partition, new Path(temp, partition + ".db"));
				LOG.info("Initializing SQL connection [" + partition + "]");

				// temp files to File(".") ?

				int port = EmbeddedMySQL.getNextAvailablePort();
				File mysqlDir = new File(mysqlDb.toString());
				LOG.info("Going to instantiate a MySQLD in: " + mysqlDir + ", port [" + port + "] (partition: "
				    + partition + ")");

				EmbeddedMySQLConfig config = new EmbeddedMySQLConfig(port, EmbeddedMySQLConfig.DEFAULT_USER,
				    EmbeddedMySQLConfig.DEFAULT_PASS, mysqlDir, null);
				EmbeddedMySQL mySQL = new EmbeddedMySQL(config);
				mySQL.start(true);
				mySQLs.put(partition, mySQL);

				// MySQL is successfully started at this point, or an Exception would have been thrown.
				Class.forName(MySQLManager.DRIVER);
				Connection conn = DriverManager.getConnection(config.getLocalJDBCConnection(GENERATED_DB_NAME),
				    config.getUser(), config.getPass());
				conn.setAutoCommit(false);
				connCache.put(partition, conn);
				Statement st = conn.createStatement();

				// Init transaction
				for(String sql : getPreSQL()) {
					LOG.info("Executing: " + sql);
					st.execute(sql);
				}
				st.execute("BEGIN");
				st.close();

				Map<String, PreparedStatement> stMap = new HashMap<String, PreparedStatement>();
				stCache.put(partition, stMap);
			} catch(Exception e) {
				throw new IOException(e);
			}
		}

		@Override
		public void write(ITuple tuple, NullWritable ignore) throws IOException, InterruptedException {
			int partition = (Integer) tuple.get(PARTITION_TUPLE_FIELD);

			try {
				/*
				 * Key performance trick: Cache PreparedStatements when possible. We will have one PreparedStatement per each
				 * different Tuple Schema (table).
				 */
				Map<String, PreparedStatement> stMap = stCache.get(partition);
				if(stMap == null) {
					initSql(partition);
					stMap = stCache.get(partition);
				}

				PreparedStatement pS = stMap.get(tuple.getSchema().getName());
				if(pS == null) {
					Connection conn = connCache.get(partition);
					// Create a PreparedStatement according to the received Tuple
					String preparedStatement = "INSERT INTO " + tuple.getSchema().getName() + " VALUES (";
					// NOTE: tuple.getSchema().getFields().size() - 1 : quick way of skipping "_partition" fields here
					for(int i = 0; i < tuple.getSchema().getFields().size() - 1; i++) {
						preparedStatement += "?, ";
					}
					preparedStatement = preparedStatement.substring(0, preparedStatement.length() - 2) + ");";
					pS = conn.prepareStatement(preparedStatement);
					stMap.put(tuple.getSchema().getName(), pS);
				}

				int count = 1, tupleCount = 0;
				for(Field field : tuple.getSchema().getFields()) {
					if(field.getName().equals(PARTITION_TUPLE_FIELD)) {
						tupleCount++;
						continue;
					}
					if(field.getType().equals(Type.STRING)) {
						pS.setObject(count, tuple.getString(tupleCount));
					} else {
						pS.setObject(count, tuple.get(tupleCount));
					}
					count++;
					tupleCount++;
				}
				pS.execute();

				records++;
				if(records == getBatchSize()) {
					Connection conn = connCache.get(partition);
					Statement st = conn.createStatement();
					st.execute("COMMIT");
					st.execute("BEGIN");
					st.close();
					records = 0;
				}
			} catch(Exception e) {
				throw new IOException(e);
			}
		}

		@Override
		public void close(TaskAttemptContext ctx) throws IOException, InterruptedException {
			try {
				if(ctx != null) {
					heartBeater.setProgress(ctx);
				}
				for(Map.Entry<Integer, Connection> entry : connCache.entrySet()) {
					LOG.info("Closing SQL connection [" + entry.getKey() + "]");
					//
					Connection conn = entry.getValue();
					Statement st = conn.createStatement();
					st.execute("COMMIT");
					if(getPostSQL() != null) {
						LOG.info("Executing end SQL statements.");
						for(String sql : getPostSQL()) {
							LOG.info("Executing: " + sql);
							st.execute(sql);
						}
					}
					st.close();
					conn.close();
					// close MySQL before copying files (so mysql.sock disappears!)
					EmbeddedMySQL msql = mySQLs.get(entry.getKey());

					msql.stop();
					File resident = msql.getConfig().getResidentFolder();
					File zipDest = new File(resident.getParentFile(), entry.getKey() + ".db");

					// Create a "partition.db" zip with the needed files.
					CompressorUtil.createZip(
					    resident,
					    zipDest,
					    new WildcardFileFilter(new String[] { "ib*", "*.frm" }),
					    FileFilterUtils.or(FileFilterUtils.nameFileFilter("data"),
					        FileFilterUtils.nameFileFilter("splout")));
					// Delete all files except the generated zip "partition.db"
					FileUtils.deleteDirectory(new File(resident, "bin"));
					FileUtils.deleteDirectory(new File(resident, "data"));
					FileUtils.deleteDirectory(new File(resident, "share"));

					// Hadoop - completeLocalOutput()

					fs.completeLocalOutput(permPool.get(entry.getKey()), tempPool.get(entry.getKey()));
				}
			} catch(Exception e) {
				throw new IOException(e);
			} finally { // in any case, destroy the HeartBeater
				for(Map.Entry<Integer, EmbeddedMySQL> entry : mySQLs.entrySet()) {
					entry.getValue().stop();
				}
				heartBeater.cancelHeartBeat();
			}
		}
	}
}
