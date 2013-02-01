package com.splout.db.dnode;

/*
 * #%L
 * Splout SQL Server
 * %%
 * Copyright (C) 2012 - 2013 Datasalt Systems S.L.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.BindException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32;
import java.util.zip.Checksum;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.splout.db.common.SploutConfiguration;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

/**
 * A simple class that allows for fast (GZip), chunked transport of binary files between nodes through HTTP. This class
 * is both a server and a client: use its Runnable method for creating a server that can receive files or use the
 * {@link #send(File, String)} method for sending a file to another peer.
 * <p>
 * For safety, every transfer checks whether the checksum (CRC32) matches the expected one or not.
 * <p>
 * This class should have the same semantics as {@link Fetcher} so it should save files to the same temp folder, etc. It
 * can be configured by {@link SploutConfiguration}.
 */
public class HttpFileExchanger extends Thread implements HttpHandler {

	private final static Log log = LogFactory.getLog(HttpFileExchanger.class);

	private File tempDir;
	private SploutConfiguration config;

	// this thread pool is passed to the HTTP server for handling incoming requests
	private ExecutorService serverExecutors;
	// this thread pool is used for sending multiple files at the same time
	private ExecutorService clientExecutors;
	private HttpServer server;

	private AtomicBoolean isListening = new AtomicBoolean(false);
	private AtomicBoolean isInit = new AtomicBoolean(false);

	// This callback will be called when the files are received
	private ReceiveFileCallback callback;

	public HttpFileExchanger(SploutConfiguration config, ReceiveFileCallback callback) {
		this.config = config;
		this.callback = callback;
	}

	public interface ReceiveFileCallback {

		public void onProgress(String tablespace, Integer partition, Long version, File file, long totalSize, long sizeDownloaded);

		public void onFileReceived(String tablespace, Integer partition, Long version, File file);

		public void onBadCRC(String tablespace, Integer partition, Long version, File file);

		public void onError(String tablespace, Integer partition, Long version, File file);
	}

	/**
	 * We initialize everything in an init() method to be able to catch explicit exceptions (otherwise that's not possible
	 * in Thread's run()).
	 */
	public void init() throws IOException {
		tempDir = new File(config.getString(FetcherProperties.TEMP_DIR));
		int httpPort = 0;
		int trials = 0;
		boolean bind = false;
		do {
			try {
				httpPort = config.getInt(HttpFileExchangerProperties.HTTP_PORT);
				server = HttpServer.create(new InetSocketAddress(config.getString(DNodeProperties.HOST),
				    httpPort), config.getInt(HttpFileExchangerProperties.HTTP_BACKLOG));
				bind = true;
			} catch(BindException e) {
				if(config.getBoolean(HttpFileExchangerProperties.HTTP_PORT_AUTO_INCREMENT)) {
					config.setProperty(HttpFileExchangerProperties.HTTP_PORT, httpPort + 1);
				} else {
					throw e;
				}
			}
		} while(!bind && trials < 50);
		// serve all http requests at root context
		server.createContext("/", this);
		serverExecutors = Executors.newFixedThreadPool(config
		    .getInt(HttpFileExchangerProperties.HTTP_THREADS_SERVER));
		clientExecutors = Executors.newFixedThreadPool(config
		    .getInt(HttpFileExchangerProperties.HTTP_THREADS_CLIENT));
		server.setExecutor(serverExecutors);
		isInit.set(true);
	}

	public String address() {
		return "http://" + config.getString(DNodeProperties.HOST) + ":" + config.getInt(HttpFileExchangerProperties.HTTP_PORT);
	}
	
	@Override
	public void run() {
		if(!isInit.get()) {
			throw new IllegalStateException("HTTP server must be init with init() method.");
		}
		server.start();
		log.info("HTTP File exchanger LISTENING on port: "
		    + config.getInt(HttpFileExchangerProperties.HTTP_PORT));
		isListening.set(true);
	}

	public boolean isListening() {
		return isListening.get();
	}

	public void close() {
		if(server != null) {
			server.stop(1);
			serverExecutors.shutdown();
			clientExecutors.shutdown();
			log.warn("HTTP File exchanger STOPPED.");
		}
	}

	@Override
	public void handle(HttpExchange exchange) throws IOException {
		DataInputStream iS = null;
		FileOutputStream writer = null;
		File dest = null;

		String tablespace = null; 
		Integer partition = null;
		Long version = null;
		
		try {
			iS = new DataInputStream(new GZIPInputStream(exchange.getRequestBody()));
			String fileName = exchange.getRequestHeaders().getFirst("filename");
			tablespace = exchange.getRequestHeaders().getFirst("tablespace");
			partition = Integer.valueOf(exchange.getRequestHeaders().getFirst("partition"));
			version = Long.valueOf(exchange.getRequestHeaders().getFirst("version"));
			
			dest = new File(new File(tempDir, DNodeHandler.getLocalStoragePartitionRelativePath(tablespace,
			    partition, version)), fileName);

			if(!dest.getParentFile().exists()) {
				dest.getParentFile().mkdirs();
			}
			if(dest.exists()) {
				dest.delete();
			}

			writer = new FileOutputStream(dest);
			byte[] buffer = new byte[config.getInt(FetcherProperties.DOWNLOAD_BUFFER)];

			Checksum checkSum = new CRC32();

			// 1- Read file size
			long fileSize = iS.readLong();
			log.debug("Going to read file [" + fileName + "] of size: " + fileSize);
			// 2- Read file contents
			long readSoFar = 0;

			do {
				long missingBytes = fileSize - readSoFar;
				int bytesToRead = (int) Math.min(missingBytes, buffer.length);
				int read = iS.read(buffer, 0, bytesToRead);
				checkSum.update(buffer, 0, read);
				writer.write(buffer, 0, read);
				readSoFar += read;
				callback.onProgress(tablespace, partition, version, dest, fileSize, readSoFar);
			} while(readSoFar < fileSize);

			// 3- Read CRC
			long expectedCrc = iS.readLong();
			if(expectedCrc == checkSum.getValue()) {
				log.info("File [" + dest.getAbsolutePath() + "] received -> Checksum -- " + checkSum.getValue()
				    + " matches expected CRC [OK]");
				callback.onFileReceived(tablespace, partition, version, dest);
			} else {
				log.error("File received [" + dest.getAbsolutePath() + "] -> Checksum -- " + checkSum.getValue()
				    + " doesn't match expected CRC: " + expectedCrc);
				callback.onBadCRC(tablespace, partition, version, dest);
			}
		} catch(Throwable t) {
			log.error(t);
			callback.onError(tablespace, partition, version, dest);
		} finally {
			if(writer != null) {
				writer.close();
			}
			if(iS != null) {
				iS.close();
			}
		}
	}

	public void send(final String tablespace, final int partition, final long version,
	    final File binaryFile, final String url, boolean blockUntilComplete) {
		Future<?> future = clientExecutors.submit(new Runnable() {
			@Override
			public void run() {
				DataOutputStream writer = null;
				InputStream input = null;
				try {
					HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
					connection.setChunkedStreamingMode(config.getInt(FetcherProperties.DOWNLOAD_BUFFER));
					connection.setDoOutput(true);
					connection.setRequestProperty("filename", binaryFile.getName());
					connection.setRequestProperty("tablespace", tablespace);
					connection.setRequestProperty("partition", partition + "");
					connection.setRequestProperty("version", version + "");

					Checksum checkSum = new CRC32();

					writer = new DataOutputStream(new GZIPOutputStream(connection.getOutputStream()));
					// 1 - write file size
					writer.writeLong(binaryFile.length());
					writer.flush();
					// 2 - write file content
					input = new FileInputStream(binaryFile);
					byte[] buffer = new byte[config.getInt(FetcherProperties.DOWNLOAD_BUFFER)];
					long wrote = 0;
					for(int length = 0; (length = input.read(buffer)) > 0;) {
						writer.write(buffer, 0, length);
						checkSum.update(buffer, 0, length);
						wrote += length;
					}
					// 3 - add the CRC so that we can verify the download
					writer.writeLong(checkSum.getValue());
					writer.flush();
					log.info("Sent file " + binaryFile + " to " + url + " with #bytes: " + wrote
					    + " and checksum: " + checkSum.getValue());
				} catch(IOException e) {
					log.error(e);
				} finally {
					try {
						if(input != null) {
							input.close();
						}
						if(writer != null) {
							writer.close();
						}
					} catch(IOException ignore) {
					}
				}
			}
		});
		try {
			if(blockUntilComplete) {
				while(future.isDone() || future.isCancelled()) {
					Thread.sleep(1000);
				}
			}
		} catch(InterruptedException e) {
			// interrupted!
		}
	}
}