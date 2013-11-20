package com.splout.db.engine;

/*
 * #%L
 * Splout SQL commons
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
import java.net.ServerSocket;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.io.Files;
import com.mysql.management.MysqldResource;
import com.mysql.management.MysqldResourceI;

/**
 * An interface to use MySQL(d) from Java in an embedded way.
 * This actually starts Mysqld in a separate process with its own PID.
 * <p>
 * Classes used: http://dev.mysql.com/doc/connector-mxj/en/connector-mxj-configuration-java-object.html 
 */
public class EmbeddedMySQL {

	private final static Log log = LogFactory.getLog(EmbeddedMySQL.class);

	public static class EmbeddedMySQLConfig {

		public final static int DEFAULT_PORT = 4567;
		public final static String DEFAULT_USER = "splout";
		public final static String DEFAULT_PASS = "splout";
		public final static File DEFAULT_RESIDENT_FOLDER = new File(System.getProperty("java.io.tmpdir"),
		    "mysql-splout");

		final int port;
		final String user;
		final String pass;
		final File residentFolder;
		final Map<String, Object> customConfig;

		public EmbeddedMySQLConfig() {
			this(DEFAULT_PORT, DEFAULT_USER, DEFAULT_PASS, DEFAULT_RESIDENT_FOLDER, null);
		}

		public EmbeddedMySQLConfig(int port, String user, String pass, File residentFolder,
		    Map<String, Object> customConfig) {
			this.port = port;
			this.user = user;
			this.pass = pass;
			this.residentFolder = residentFolder;
			this.customConfig = customConfig;
		}

		public Map<String, Object> getCustomConfig() {
			return customConfig;
		}

		public String getPass() {
			return pass;
		}

		public int getPort() {
			return port;
		}

		public File getResidentFolder() {
			return residentFolder;
		}

		public String getUser() {
			return user;
		}
		
		/**
		 * Normal JDBC connection string to localhost with "createDatabaseIfNotExist"
		 */
		public String getLocalJDBCConnection(String dbName) {
			return "jdbc:mysql://localhost:" + port + "/" + dbName + "?createDatabaseIfNotExist=true";
		}
		
		@Override
		public String toString() {
			return ReflectionToStringBuilder.toString(this);
		}
	};

	final EmbeddedMySQLConfig config;
	MysqldResource resource = null;

	public EmbeddedMySQL() {
		this(new EmbeddedMySQLConfig());
	}
	
	public EmbeddedMySQLConfig getConfig() {
	  return config;
  }
	
	public EmbeddedMySQL(EmbeddedMySQLConfig config) {
		this.config = config;
	}

	/**
	 * It's ok to call this multiple times (redundant times will be ignored). 
	 */
	public void stop() {
		if(resource != null) {
			if(resource.isRunning()) {
				resource.shutdown();
				resource = null;
			}
		} else {
			log.warn("Nothing to stop.");
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void start(boolean deleteFilesIfExist) throws IOException, InterruptedException {
		if(deleteFilesIfExist && config.residentFolder.exists()) {
			File pidFile = new File(config.residentFolder, "data/MysqldResource.pid");
			if(pidFile.exists()) {
				// Issue "kill -9" if process is still alive
				String pid  = Files.toString(pidFile, Charset.defaultCharset());
				log.info("Killing existing process: " + pid);
				Runtime.getRuntime().exec("kill -9 " + pid).waitFor();
			}
			log.info("Deleting contents of: " + config.residentFolder);
			FileUtils.deleteDirectory(config.residentFolder);
		}
		log.info("Using config: " + config);
		MysqldResource mysqldResource = new MysqldResource(config.residentFolder);
		Map database_options = new HashMap();
		database_options.put(MysqldResourceI.PORT, Integer.toString(config.port));
		database_options.put(MysqldResourceI.INITIALIZE_USER, "true");
		database_options.put(MysqldResourceI.INITIALIZE_USER_NAME, config.user);
		database_options.put(MysqldResourceI.INITIALIZE_PASSWORD, config.pass);
		if(config.customConfig != null) {
			for(Map.Entry<String, Object> entry : config.customConfig.entrySet()) {
				database_options.put(entry.getKey(), entry.getValue());
			}
		}
		// I have to do this checking myself, otherwise in some cases mysqldResource will block undefinitely...
		try {
			ServerSocket serverSocket = new ServerSocket(config.port);
			serverSocket.close();
		} catch (IOException e) {
	    throw new RuntimeException("Port already in use: " + config.port);
		}
		if(mysqldResource.isRunning()) {
			throw new RuntimeException("MySQL already running!");
		}
		mysqldResource.start("test-mysqld-thread", database_options);
		if(!mysqldResource.isRunning()) {
			throw new RuntimeException("MySQL did not start successfully!");
		}
		log.info("MySQL is running.");
		resource = mysqldResource;
	}
}
