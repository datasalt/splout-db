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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.FileChannel;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

import com.splout.db.common.SploutConfiguration;

/**
 * This Fetcher is used by {@link DNodeHandler} to fetch data to deploy. It handles: file, HDFS and S3 URIs. For the S3
 * service it uses the JETS3T library.
 * <p>
 * The fetcher has to return a local File object which is a folder that can be used to perform an atomic "mv" operation.
 * The folder has to contain the DB file.
 */
public class Fetcher {

	private final static Log log = LogFactory.getLog(Fetcher.class);

	private File tempDir;
	private S3Service s3Service;
	private String accessKey;
	private String secretKey;
	private int downloadBufferSize;
	private int bytesPerSecThrottle;
	private long bytesToReportProgress;

	private Configuration hadoopConf;

	public final static int SIZE_UNKNOWN = -1;

	public Fetcher(SploutConfiguration config) {
		tempDir = new File(config.getString(FetcherProperties.TEMP_DIR));
		accessKey = config.getString(FetcherProperties.S3_ACCESS_KEY, null);
		secretKey = config.getString(FetcherProperties.S3_SECRET_KEY, null);
		downloadBufferSize = config.getInt(FetcherProperties.DOWNLOAD_BUFFER);
		bytesPerSecThrottle = config.getInt(FetcherProperties.BYTES_PER_SEC_THROTTLE);
		bytesToReportProgress = config.getLong(FetcherProperties.BYTES_TO_REPORT_PROGRESS);
		String fsName = config.getString(FetcherProperties.HADOOP_FS_NAME);
		hadoopConf = new Configuration();
		if(fsName != null) {
			hadoopConf.set("fs.default.name", fsName);
		}
		log.info("Created " + Fetcher.class + " with tempDir = " + tempDir);
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
	private File hdfsFetch(Path fromPath, Reporter reporter) throws IOException {
    UUID uniqueId = UUID.randomUUID();
		File toFile = new File(tempDir, uniqueId.toString());
		File toDir = new File(toFile.getParent());
		if(toDir.exists()) {
			FileUtils.deleteDirectory(toDir);
		}
		toDir.mkdirs();
		Path toPath = new Path(toFile.getCanonicalPath());

		FileSystem fS = fromPath.getFileSystem(hadoopConf);
		FileSystem tofS = FileSystem.getLocal(hadoopConf);

		Throttler throttler = new Throttler((double) bytesPerSecThrottle);

		for(FileStatus fStatus : fS.globStatus(fromPath)) {
			log.info("Copying " + fStatus.getPath() + " to " + toPath);
			long bytesSoFar = 0;

			FSDataInputStream iS = fS.open(fStatus.getPath());
			FSDataOutputStream oS = tofS.create(toPath);

			byte[] buffer = new byte[downloadBufferSize];

			int nRead;
			while((nRead = iS.read(buffer, 0, buffer.length)) != -1) {
				bytesSoFar += nRead;
				oS.write(buffer, 0, nRead);
				throttler.incrementAndThrottle(nRead);
				if(bytesSoFar >= bytesToReportProgress) {
					reporter.progress(bytesSoFar);
					bytesSoFar = 0l;
				}
			}

			if(reporter != null) {
				reporter.progress(bytesSoFar);
			}

			oS.close();
			iS.close();
		}

		return toDir;
	}

	/**
	 * An interface that can be implemented to receive progress about fetching. The Fetcher will use it when provided.
	 **/
	public static interface Reporter {

		/**
		 * This method is called periodically to report progress made. The reported consumed bytes are an incremental
		 * measure, not a total measure. In other words, "consumed" is not the total consumed bytes since the beginning of
		 * the fetching, but the total "consumed" bytes since the last call to progress.
		 */
		public void progress(long consumed);
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
	private File s3Fetch(URI uri, Reporter reporter) throws IOException {
		String bucketName = uri.getHost();
		String path = uri.getPath();
    UUID uniqueId = UUID.randomUUID();
		File destFolder = new File(tempDir, uniqueId.toString());
		if(destFolder.exists()) {
			FileUtils.deleteDirectory(destFolder);
		}
		destFolder.mkdirs();

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
				long bytesSoFar = 0;

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
					bytesSoFar += nRead;
					writer.write(buffer, 0, nRead);
					throttler.incrementAndThrottle(nRead);
					if(bytesSoFar >= bytesToReportProgress) {
						reporter.progress(bytesSoFar);
						bytesSoFar = 0l;
					}
				}

				if(reporter != null) {
					reporter.progress(bytesSoFar);
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
	private File fileFetch(File file, Reporter reporter) throws IOException {
    UUID uniqueId = UUID.randomUUID();
		File toDir = new File(tempDir, uniqueId.toString());
		if(toDir.exists()) {
			FileUtils.deleteDirectory(toDir);
		}
		toDir.mkdirs();
		log.info("Copying " + file + " to " + toDir);
		copyFile(file, new File(toDir, file.getName()), reporter);
		return toDir;
	}

	private void copyFile(File sourceFile, File destFile, Reporter reporter) throws IOException {
		if(!destFile.exists()) {
			destFile.createNewFile();
		}
		FileChannel source = null;
		FileChannel destination = null;

		Throttler throttler = new Throttler((double) bytesPerSecThrottle);

		FileInputStream iS = null;
		FileOutputStream oS = null;

		try {
			iS = new FileInputStream(sourceFile);
			oS = new FileOutputStream(destFile);
			source = iS.getChannel();
			destination = oS.getChannel();
			long bytesSoFar = 0;
			long reportingBytesSoFar = 0;
			long size = source.size();

			int transferred = 0;

			while(bytesSoFar < size) {
				// Casting to int here is safe since we will transfer at most "downloadBufferSize" bytes.
				// This is done on purpose for being able to implement Throttling.
				transferred = (int) destination.transferFrom(source, bytesSoFar, downloadBufferSize);
				bytesSoFar += transferred;
				reportingBytesSoFar += transferred;
				throttler.incrementAndThrottle(transferred);
				if(reportingBytesSoFar >= bytesToReportProgress) {
					reporter.progress(reportingBytesSoFar);
					reportingBytesSoFar = 0l;
				}
			}

			if(reporter != null) {
				reporter.progress(reportingBytesSoFar);
			}

		} finally {
			if(iS != null) {
				iS.close();
			}
			if(oS != null) {
				oS.close();
			}
			if(source != null) {
				source.close();
			}
			if(destination != null) {
				destination.close();
			}
		}
	}

	/**
	 * Use this method to know the total size of a deployment URI.
	 */
	public long sizeOf(String uriStr) throws IOException, URISyntaxException {
		URI uri = new URI(uriStr);
		if(uriStr.startsWith("file:")) {
			File f = new File(uri);
			return f.isDirectory() ? FileUtils.sizeOfDirectory(f) : f.length();
		} else if(uriStr.startsWith("s3")) {
			return -1; // NotYetImplemented
		} else if(uriStr.startsWith("hdfs")) {
			return FileSystem.get(hadoopConf).getContentSummary(new Path(uriStr)).getLength();
		} else {
			throw new IllegalArgumentException("Scheme not recognized or non-absolute URI provided: " + uri);
		}
	}

	/**
	 * This is the main method that accepts a URI string and delegates the fetching to the appropriate private method.
	 */
	public File fetch(String uriStr) throws IOException, URISyntaxException {
		return fetch(uriStr, null);
	}

	/**
	 * This is the main method that accepts a URI string and delegates the fetching to the appropriate private method.
	 */
	public File fetch(String uriStr, Reporter reporter) throws IOException, URISyntaxException {
		URI uri = new URI(uriStr);
		if(uriStr.startsWith("file:")) {
			return fileFetch(new File(uri), reporter);
		} else if(uriStr.startsWith("s3")) {
			return s3Fetch(uri, reporter);
		} else if(uriStr.startsWith("hdfs")) {
			return hdfsFetch(new Path(uriStr), reporter);
		} else {
			throw new IllegalArgumentException("Scheme not recognized or non-absolute URI provided: " + uri);
		}
	}
}
