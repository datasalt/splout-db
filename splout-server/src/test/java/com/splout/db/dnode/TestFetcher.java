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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.nio.charset.Charset;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.google.common.io.Files;
import com.splout.db.common.SploutConfiguration;
import com.splout.db.dnode.Fetcher.Throttler;

public class TestFetcher {

	@Test
	public void testThrottling() throws InterruptedException {
		double bytesPerSec = 1000;
		Throttler throttler = new Throttler(bytesPerSec);
		long startTime = System.currentTimeMillis();
		
		int bytesConsumed = 0;
		
		for(int i = 0; i < 10; i++) {
			int bytes = (int)(Math.random() * 1000);
			bytesConsumed += bytes;
			throttler.incrementAndThrottle(bytes);	
		}
		
		long endTime = System.currentTimeMillis();
		double secs = (endTime - startTime) / (double)1000;
		double avgBytesPerSec = bytesConsumed / secs;
		
		assertEquals(bytesPerSec, avgBytesPerSec, 5.0); // + - 5
	}
	
	@Test
	public void testHdfsFetching() throws IOException, URISyntaxException {
		Configuration conf = new Configuration();
		FileSystem fS = FileSystem.getLocal(conf);
		
		SploutConfiguration testConfig = SploutConfiguration.getTestConfig();
		testConfig.setProperty(FetcherProperties.TEMP_DIR, "tmp-dir-" + TestFetcher.class.getName());
		Fetcher fetcher = new Fetcher(testConfig);
		
		Path path = new Path("tmp-" + TestFetcher.class.getName());
		OutputStream oS = fS.create(path);
		oS.write("This is what happens when you don't know what to write".getBytes());
		oS.close();
		
		File f = fetcher.fetch(new Path(fS.getWorkingDirectory(), path.getName()).toUri().toString());
		
		assertTrue(f.exists());
		assertTrue(f.isDirectory());
		
		File file = new File(f, "tmp-" + TestFetcher.class.getName());
		assertTrue(file.exists());
		
		assertEquals("This is what happens when you don't know what to write", Files.toString(file, Charset.defaultCharset()));
		
		fS.delete(path, true);
		FileUtils.deleteDirectory(f);
	}
	
	@Test
	public void testHdfsFetchingAndThrottling() throws IOException, URISyntaxException {
		Configuration conf = new Configuration();
		FileSystem fS = FileSystem.getLocal(conf);
		
		SploutConfiguration testConfig = SploutConfiguration.getTestConfig();
		testConfig.setProperty(FetcherProperties.TEMP_DIR, "tmp-dir-" + TestFetcher.class.getName());
		testConfig.setProperty(FetcherProperties.DOWNLOAD_BUFFER, 4);
		testConfig.setProperty(FetcherProperties.BYTES_PER_SEC_THROTTLE, 8);
		Fetcher fetcher = new Fetcher(testConfig);
		
		final String str = "This is what happens when you don't know what to write"; 
		
		Path path = new Path("tmp-" + TestFetcher.class.getName());
		OutputStream oS = fS.create(path);
		oS.write(str.getBytes());
		oS.close();
		
		long startTime = System.currentTimeMillis();
		File f = fetcher.fetch(new Path(fS.getWorkingDirectory(), path.getName()).toUri().toString());
		long endTime = System.currentTimeMillis();
		
		double bytesPerSec = (str.getBytes().length / (double)(endTime - startTime)) * 1000;
		assertEquals(8, bytesPerSec, 0.1);
		
		assertTrue(f.exists());
		assertTrue(f.isDirectory());
		
		File file = new File(f, "tmp-" + TestFetcher.class.getName());
		assertTrue(file.exists());
		
		assertEquals(str, Files.toString(file, Charset.defaultCharset()));
		
		fS.delete(path, true);
		FileUtils.deleteDirectory(f);
	}
	
	@Test
	public void testFileFetching() throws IOException, URISyntaxException {
		SploutConfiguration testConfig = SploutConfiguration.getTestConfig();
		testConfig.setProperty(FetcherProperties.TEMP_DIR, "tmp-dir-" + TestFetcher.class.getName());
		Fetcher fetcher = new Fetcher(testConfig);
		
		File file = new File("tmp-" + TestFetcher.class.getName());
		Files.write("This is what happens when you don't know what to write".getBytes(), file);
		
		File f = fetcher.fetch(file.getAbsoluteFile().toURI().toString());
		
		assertTrue(f.exists());
		assertTrue(f.isDirectory());
		
		File file2 = new File(f, "tmp-" + TestFetcher.class.getName());
		assertTrue(file.exists());
		
		assertEquals("This is what happens when you don't know what to write", Files.toString(file2, Charset.defaultCharset()));
		
		file.delete();
		FileUtils.deleteDirectory(f);
	}
	
	@Test
	public void testFileFetchingAndThrottling() throws IOException, URISyntaxException {
		SploutConfiguration testConfig = SploutConfiguration.getTestConfig();
		testConfig.setProperty(FetcherProperties.TEMP_DIR, "tmp-dir-" + TestFetcher.class.getName());
		testConfig.setProperty(FetcherProperties.DOWNLOAD_BUFFER, 4);
		testConfig.setProperty(FetcherProperties.BYTES_PER_SEC_THROTTLE, 8);
		Fetcher fetcher = new Fetcher(testConfig);
		
		final String str = "This is what happens when you don't know what to write"; 
		
		File file = new File("tmp-" + TestFetcher.class.getName());
		Files.write(str.getBytes(), file);
		
		long startTime = System.currentTimeMillis();
		File f = fetcher.fetch(file.getAbsoluteFile().toURI().toString());
		long endTime = System.currentTimeMillis();
		
		double bytesPerSec = (str.getBytes().length / (double)(endTime - startTime)) * 1000;
		assertEquals(8, bytesPerSec, 0.1);
		
		assertTrue(f.exists());
		assertTrue(f.isDirectory());
		
		File file2 = new File(f, "tmp-" + TestFetcher.class.getName());
		assertTrue(file.exists());
		
		assertEquals(str, Files.toString(file2, Charset.defaultCharset()));
		
		file.delete();
		FileUtils.deleteDirectory(f);
	}
}
