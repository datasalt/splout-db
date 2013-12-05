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
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.splout.db.common.CompressorUtil;
import com.splout.db.engine.EmbeddedMySQL;
import com.splout.db.engine.EmbeddedMySQL.EmbeddedMySQLConfig;
import com.splout.db.engine.MySQLManager;
import com.splout.db.engine.PortUtils;
import com.splout.db.engine.PortUtils.PortLock;
import com.splout.db.hadoop.TableSpec;

@SuppressWarnings("serial")
public class MySQLOutputFormat extends SploutSQLOutputFormat implements Serializable {

	public static Log LOG = LogFactory.getLog(MySQLOutputFormat.class);

	public static String STRING_FIELD_SIZE_PROP = "com.splout.db.hadoop.engine.MySQLOutputFormat.string.field.size";
	public static String AUTO_TRIM_STRING = "com.splout.db.hadoop.engine.MySQLOutputFormat.auto.trim.string";

	public static String GENERATED_DB_NAME = "splout";

	private String engine;
	private String charset;

	// Keep track of all opened Mysqlds so we can kill them in any case
	private Map<Integer, EmbeddedMySQL> mySQLs = new HashMap<Integer, EmbeddedMySQL>();

	public MySQLOutputFormat(int batchSize, TableSpec... dbSpec) throws SploutSQLOutputFormatException {
		this(batchSize, "MyISAM", "UTF8", dbSpec);
	}

	public MySQLOutputFormat(int batchSize, String engine, String charset, TableSpec... dbSpec)
	    throws SploutSQLOutputFormatException {
		super(batchSize, dbSpec);
		this.engine = engine;
		this.charset = charset;
		createPrePostSQL();
	}

	public void setEngine(String engine) {
		this.engine = engine;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public String getEngine() {
		return engine;
	}

	public String getCharset() {
		return charset;
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
		return createTable += ") ENGINE=" + engine + " DEFAULT CHARSET=" + charset;
	}

	// Map of prepared statements per Schema and per Partition
	private Map<Integer, Map<String, PreparedStatement>> stCache = new HashMap<Integer, Map<String, PreparedStatement>>();
	private Map<Integer, Connection> connCache = new HashMap<Integer, Connection>();

	private long records = 0;

	// This method is called one time per each partition
	public void initPartition(int partition, Path local) throws IOException {

		Path mysqlDb = new Path(local.getParent(), partition + "");

		LOG.info("Initializing SQL connection [" + partition + "]");
		try {
			PortLock portLock = PortUtils.getNextAvailablePort(EmbeddedMySQLConfig.DEFAULT_PORT);

			EmbeddedMySQL mySQL = null;
			EmbeddedMySQLConfig config = null;

			try {
				File mysqlDir = new File(mysqlDb.toString());
				LOG.info("Going to instantiate a MySQLD in: " + mysqlDir + ", port [" + portLock.getPort()
				    + "] (partition: " + partition + ")");

				config = new EmbeddedMySQLConfig(portLock.getPort(), EmbeddedMySQLConfig.DEFAULT_USER,
				    EmbeddedMySQLConfig.DEFAULT_PASS, mysqlDir, null);
				mySQL = new EmbeddedMySQL(config);
				mySQL.start(true);
			} catch(Exception e) {
				throw e;
			} finally {
				portLock.release();
			}

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
	public void write(ITuple tuple) throws IOException, InterruptedException {
		int partition = (Integer) tuple.get(PARTITION_TUPLE_FIELD);

		try {
			/*
			 * Key performance trick: Cache PreparedStatements when possible. We will have one PreparedStatement per each
			 * different Tuple Schema (table).
			 */
			Map<String, PreparedStatement> stMap = stCache.get(partition);

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
					boolean autoTrim = false;
					if(field.getProp(AUTO_TRIM_STRING) != null) {
						autoTrim = true;
					}
					int fieldSize = -1;
					if(field.getProp(STRING_FIELD_SIZE_PROP) != null) {
						fieldSize = Integer.parseInt(field.getProp(STRING_FIELD_SIZE_PROP));
					}
					String str = tuple.getString(tupleCount);
					if(fieldSize > 0 && autoTrim && str != null && str.length() > fieldSize) {
						str = str.substring(0, Math.min(fieldSize, str.length()));
					}
					pS.setObject(count, str);
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
	public void close() throws IOException, InterruptedException {
		try {
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
				    new WildcardFileFilter(new String[] { "ib*", "*.frm", "*.MYD", "*.MYI", "db.opt" }),
				    FileFilterUtils.or(FileFilterUtils.nameFileFilter("data"),
				        FileFilterUtils.nameFileFilter("splout")));
				// Delete all files except the generated zip "partition.db"
				FileUtils.deleteDirectory(new File(resident, "bin"));
				FileUtils.deleteDirectory(new File(resident, "data"));
				FileUtils.deleteDirectory(new File(resident, "share"));
			}
		} catch(Exception e) {
			throw new IOException(e);
		} finally { // in any case, destroy the HeartBeater
			for(Map.Entry<Integer, EmbeddedMySQL> entry : mySQLs.entrySet()) {
				entry.getValue().stop();
			}
		}
	}

	@Override
  public void init(Configuration conf) throws IOException, InterruptedException {
	  
  }
}
