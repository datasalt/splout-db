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

/**
 * All the {@link com.splout.db.common.SploutConfiguration} properties related to the {@link com.splout.db.dnode.Fetcher} of the {@link com.splout.db.dnode.DNode}.
 */
public class FetcherProperties {

	/**
	 * The local folder that will be used to download new deployments
	 */
	public final static String TEMP_DIR = "fetcher.temp.dir";
	/**
	 * The AWS credentials
	 */
	public final static String S3_ACCESS_KEY = "fetcher.s3.access.key";
	/**
	 * The AWS credentials
	 */
	public final static String S3_SECRET_KEY = "fetcher.s3.secret.key";
	/**
	 * The size in bytes of the in-memory buffer used to download files
	 */
	public final static String DOWNLOAD_BUFFER = "fetcher.download.buffer";
	/**
	 * The address of the NameNode for being able download data from HDFS 
	 */
	public final static String HADOOP_FS_NAME = "fetcher.hadoop.fs.name";
	/**
	 * The number of bytes per sec to limit downloading for not impacting database servicing
	 */
	public final static String BYTES_PER_SEC_THROTTLE = "fetcher.bytes.per.sec.throttle";
	/**
	 * Everytime this number of bytes have been fetched from a file, a report will be made so that
	 * the DNode can calculate the transferring speed and put the info into Hazelcast.
	 * This shouldn't be too low - otherwise it would overload the network!
	 * Leave default value in case of doubt.
	 */
	public final static String BYTES_TO_REPORT_PROGRESS = "fetcher.bytes.to.report.progress";
}
