package com.splout.db.dnode;

/*
 * #%L
 * Splout SQL Server
 * %%
 * Copyright (C) 2012 Datasalt Systems S.L.
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

import com.splout.db.common.SploutConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.FileChannel;

/**
 * This Fetcher is used by {@link DNodeHandler} to fetch data to deploy. It handles: file, HDFS and S3 URIs. For the S3
 * service it uses the JETS3T library.
 * <p>
 * The fetcher has to return a local File object which is a folder that can be used to perform an atomic "mv" operation.
 * The folder has to contain the DB file.
 */
public class Fetcher {

	private final static Log log = LogFactory.getLog(Fetcher.class);

	File tempDir;
	S3Service s3Service;
	String accessKey;
	String secretKey;
	int downloadBufferSize;
	int bytesPerSecThrottle;

	Configuration hadoopConf;

	public Fetcher(SploutConfiguration config) {
		tempDir = new File(config.getString(FetcherProperties.TEMP_DIR));
		accessKey = config.getString(FetcherProperties.S3_ACCESS_KEY, null);
		secretKey = config.getString(FetcherProperties.S3_SECRET_KEY, null);
		downloadBufferSize = config.getInt(FetcherProperties.DOWNLOAD_BUFFER);
		bytesPerSecThrottle = config.getInt(FetcherProperties.BYTES_PER_SEC_THROTTLE);
		String fsName = config.getString(FetcherProperties.HADOOP_FS_NAME);
		hadoopConf = new Configuration();
		if(fsName != null) {
			hadoopConf.set("fs.default.name", fsName);
		}
		log.info("Created " + Fetcher.class + " with tempDir = " + tempDir );
		if(bytesPerSecThrottle > 0) {
			log.info("Throttling at: " + bytesPerSecThrottle + " bytes per sec.");
		} else {
			log.warn("No throttling. Fetched data will be transferred at full speed. This may affect query servicing.");
		}
	}

	private AWSCredentials getCredentials() {
		AWSCredentials credentials = new AWSCredentials(accessKey, secretKey);
		return credentials;
	}

	/*
	 * Fetch a file that is in a Hadoop file system. Return a local File.
	 */
	private File hdfsFetch(String path) throws IOException {
		Path fromPath = new Path(path);
		File toFile = new File(tempDir, fromPath.toUri().getPath());
		File toDir = new File(toFile.getParent());
		if(!toDir.exists()) {
			toDir.mkdirs();
		}
		Path toPath = new Path(toFile.getCanonicalPath());
		
		FileSystem fS = fromPath.getFileSystem(hadoopConf);
		FileSystem tofS = FileSystem.getLocal(hadoopConf);

		Throttler throttler = new Throttler((double) bytesPerSecThrottle);

		for(FileStatus fStatus : fS.globStatus(fromPath)) {
			log.info("Copying " + fStatus.getPath() + " to " + toPath);

			FSDataInputStream iS = fS.open(fStatus.getPath());
			FSDataOutputStream oS = tofS.create(toPath);

			byte[] buffer = new byte[downloadBufferSize];

			int nRead;
			while((nRead = iS.read(buffer, 0, buffer.length)) != -1) {
				oS.write(buffer, 0, nRead);
				throttler.incrementAndThrottle(nRead);
			}

			oS.close();
		}
		return toDir;
	}

	/**
	 * Implements basic throttling capabilities.
	 */
	public static class Throttler {

		double bytesPerSec;
		long lastTime = System.currentTimeMillis();

		public Throttler(double bytesPerSec) {
			this.bytesPerSec = bytesPerSec;
		}

		public void incrementAndThrottle(int bytes) {
			if(bytesPerSec < 1) { // no throttle at all
				return;
			}
			long currentTime = System.currentTimeMillis();
			long timeDiff = currentTime - lastTime;
			if(timeDiff == 0) {
				timeDiff = 1;
			}

			double bytesPerSec = (bytes / (double) timeDiff) * 1000;
			if(bytesPerSec > this.bytesPerSec) {
				// Throttle
				double exceededByFactorOf = bytesPerSec / this.bytesPerSec;
				try {
					long mustSleep = (long) ((exceededByFactorOf - 1) * timeDiff);
					Thread.sleep(mustSleep);
				} catch(InterruptedException e) {
					e.printStackTrace();
				}
			}

			lastTime = System.currentTimeMillis();
		}
	}

	/*
	 * Fetch a file that is in a S3 file system. Return a local File. It accepts "s3://" and "s3n://" prefixes.
	 */
  private File s3Fetch(String fileUrl) throws IOException {
		URI uri;
		try {
			uri = new URI(fileUrl);
		} catch(URISyntaxException e1) {
			throw new RuntimeException("Bad URI passed to s3Fetch()! " + fileUrl);
		}

		String bucketName = uri.getHost();
		String path = uri.getPath();

		File destFolder = new File(tempDir, bucketName + "/" + path);
		if(!destFolder.exists()) {
			destFolder.mkdirs();
		}

		Throttler throttler = new Throttler((double) bytesPerSecThrottle);

		boolean done = false;
		try {
			s3Service = new RestS3Service(getCredentials());
			if(s3Service.checkBucketStatus(bucketName) != RestS3Service.BUCKET_STATUS__MY_BUCKET) {
				throw new IOException("Bucket doesn't exist or is already claimed: " + bucketName);
			}

			if(path.startsWith("/")) {
				path = path.substring(1, path.length());
			}

			for(S3Object object : s3Service.listObjects(new S3Bucket(bucketName), path, "")) {
				String fileName = path;
				if(path.contains("/")) {
					fileName = path.substring(path.lastIndexOf("/") + 1, path.length());
				}
				File fileDest = new File(destFolder, fileName);
				log.info("Downloading " + object.getKey() + " to " + fileDest + " ...");

				if(fileDest.exists()) {
					fileDest.delete();
				}

				object = s3Service.getObject(new S3Bucket(bucketName), object.getKey());
				InputStream iS = object.getDataInputStream();
				FileOutputStream writer = new FileOutputStream(fileDest);
				byte[] buffer = new byte[downloadBufferSize];

				int nRead;
				while((nRead = iS.read(buffer, 0, buffer.length)) != -1) {
					writer.write(buffer, 0, nRead);
					throttler.incrementAndThrottle(nRead);
				}

				writer.close();
				iS.close();
				done = true;
			}

			if(!done) {
				throw new IOException("Bucket is empty! " + bucketName + " path: " + path);
			}
		} catch(S3ServiceException e) {
			throw new IOException(e);
		} 

		return destFolder;
	}

	/*
	 * Fetch a file that is in a local file system. Return a local File.
	 */
	private File fileFetch(URI uri) throws IOException {
		File file = new File(uri);
		File toDir = new File(tempDir, file.getParent() + "/" + file.getName());
		if(!toDir.exists()) {
			toDir.mkdirs();
		}
		log.info("Copying " + file + " to " + toDir);
		copyFile(file, new File(toDir, file.getName()));
		return toDir;
	}

	private void copyFile(File sourceFile, File destFile) throws IOException {
		if(!destFile.exists()) {
			destFile.createNewFile();
		}
		FileChannel source = null;
		FileChannel destination = null;

		Throttler throttler = new Throttler((double) bytesPerSecThrottle);

		try {
			source = new FileInputStream(sourceFile).getChannel();
			destination = new FileOutputStream(destFile).getChannel();
			long count = 0;
			long size = source.size();

			int transferred = 0;

			while(count < size) {
				// Casting to int here is safe since we will transfer at most "downloadBufferSize" bytes.
				// This is done on purpose for being able to implement Throttling.
				transferred = (int) destination.transferFrom(source, count, downloadBufferSize);
				count += transferred;
				throttler.incrementAndThrottle(transferred);
			}
		} finally {
			if(source != null) {
				source.close();
			}
			if(destination != null) {
				destination.close();
			}
		}
	}

	/**
	 * This is the main method that accepts a URI string and delegates the fetching to the appropriate private method.
	 */
	public File fetch(String uriStr) throws IOException, URISyntaxException {
		URI uri = new URI(uriStr);
		if(uriStr.startsWith("file:")) {
			return fileFetch(uri);
		} else if(uriStr.startsWith("s3")) {
			if(uriStr.startsWith("s3n")) {
				return s3Fetch(uriStr);
			} else {
				return s3Fetch(uriStr);
			}
		} else if(uriStr.startsWith("hdfs")) {
			return hdfsFetch(uriStr);
		} else {
			throw new IllegalArgumentException("Scheme not recognized or non-absolute URI provided: " + uri);
		}
	}
}
