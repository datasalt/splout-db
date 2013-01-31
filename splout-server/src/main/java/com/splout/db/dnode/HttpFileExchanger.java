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
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
public class HttpFileExchanger implements HttpHandler, Runnable {

	private final static Log log = LogFactory.getLog(HttpFileExchanger.class);

	private File tempDir;
	private int downloadBufferSize;
	private String host;

	private int port;
	private int backlog;
	private int nThreads;

	private ExecutorService executors;
	private HttpServer server;
	private AtomicBoolean isListening = new AtomicBoolean(false);

	public HttpFileExchanger(SploutConfiguration config) {
		this(config.getString(DNodeProperties.HOST),
		    config.getInt(HttpFileExchangerProperties.HTTP_PORT), config
		        .getInt(HttpFileExchangerProperties.HTTP_THREADS), config
		        .getInt(HttpFileExchangerProperties.HTTP_BACKLOG), new File(
		        config.getString(FetcherProperties.TEMP_DIR)), config
		        .getInt(FetcherProperties.DOWNLOAD_BUFFER));
	}

	public HttpFileExchanger(String host, int port, int nThreads, int backlog, File tempDir,
	    int downloadBufferSize) {
		this.host = host;
		this.port = port;
		this.backlog = backlog;
		this.nThreads = nThreads;
		this.tempDir = tempDir;
		this.downloadBufferSize = downloadBufferSize;
	}

	@Override
	public void run() {
		try {
			server = HttpServer.create(new InetSocketAddress(host, port), backlog);
			// serve all http requests at root context
			server.createContext("/", this);
			// executor with fixed number of threads
			executors = Executors.newFixedThreadPool(nThreads);
			server.setExecutor(executors);
			server.start();
			log.info("HTTP File exchanger LISTENING on port: " + port);
			isListening.set(true);
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
	}

	public boolean isListening() {
		return isListening.get();
	}

	public void close() {
		server.stop(1);
		executors.shutdown();
		log.warn("HTTP File exchanger STOPPED.");
	}

	@Override
	public void handle(HttpExchange exchange) throws IOException {
		DataInputStream iS = new DataInputStream(new GZIPInputStream(exchange.getRequestBody()));
		FileOutputStream writer = null;
		try {
			String fileName = exchange.getRequestHeaders().getFirst("filename");

			File dest = new File(tempDir, fileName);
			if(!dest.getParentFile().exists()) {
				dest.getParentFile().mkdirs();
			}
			if(dest.exists()) {
				dest.delete();
			}

			writer = new FileOutputStream(dest);
			byte[] buffer = new byte[downloadBufferSize];

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
			} while(readSoFar < fileSize);

			// 3- Read CRC
			long expectedCrc = iS.readLong();
			if(expectedCrc == checkSum.getValue()) {
				log.info("File [" + fileName + "] received -> Checksum -- " + checkSum.getValue()
				    + " matches expected CRC [OK]");
			} else {
				log.error("File received -> Checksum -- " + checkSum.getValue()
				    + " doesn't match expected CRC: " + expectedCrc);
			}
		} finally {
			if(writer != null) {
				writer.close();
			}
			if(iS != null) {
				iS.close();
			}
		}
	}

	public void send(File binaryFile, String url) throws MalformedURLException, IOException {
		HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
		connection.setChunkedStreamingMode(downloadBufferSize);
		connection.setDoOutput(true);
		connection.setRequestProperty("filename", binaryFile.getName());

		DataOutputStream writer = null;

		Checksum checkSum = new CRC32();

		InputStream input = null;
		try {

			writer = new DataOutputStream(new GZIPOutputStream(connection.getOutputStream()));
			// 1 - write file size
			writer.writeLong(binaryFile.length());
			writer.flush();
			// 2 - write file content
			input = new FileInputStream(binaryFile);
			byte[] buffer = new byte[downloadBufferSize];
			long wrote = 0;
			for(int length = 0; (length = input.read(buffer)) > 0;) {
				writer.write(buffer, 0, length);
				checkSum.update(buffer, 0, length);
				wrote += length;
			}
			// 3 - add the CRC so that we can verify the download
			writer.writeLong(checkSum.getValue());
			writer.flush();
			log.info("Sent file " + binaryFile + " with #bytes: " + wrote + " and checksum: "
			    + checkSum.getValue());
		} finally {
			if(input != null) {
				input.close();
			}
			if(writer != null) {
				writer.close();
			}
		}
	}
}
