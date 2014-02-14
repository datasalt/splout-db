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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.Schema;
import com.google.common.io.Files;
import com.splout.db.common.JSONSerDe;
import com.splout.db.engine.SQLite4JavaClient;
import com.splout.db.hadoop.TupleSampler.SamplingType;

public class TestTablespaceGeneratorMultiplePartitionBy {
	public static String TEST_INPUT  = "in-" + TestTablespaceGeneratorMultiplePartitionBy.class.getName();
	public static String TEST_OUTPUT = "out-" + TestTablespaceGeneratorMultiplePartitionBy.class.getName();
	public static Character TAB = '\t';
	
	// ---- //
	public static Schema PAYMENTS_SCHEMA = new Schema("payments", Fields.parse("name:string, commerce:int, amount:int, currency:string"));
	public static Schema LOGS_SCHEMA = new Schema("logs", Fields.parse("name:string, commerce:int, date:string, action:string, loc:string"));
	
	// ---- //
	public static File PAYMENTS_FILE = new File(TEST_INPUT, "payments.txt");
	public static File LOGS_FILE = new File(TEST_INPUT, "logs.txt");

	@Before
	public void jLibraryPath() {
//		SploutConfiguration.setDevelopmentJavaLibraryPath();
	}
	
	@Before
	@After
	public void cleanUp() throws IOException {
		for(String cleanUpFolder: new String[] { TEST_INPUT, TEST_OUTPUT }) {
			File outFolder = new File(cleanUpFolder);
			if(outFolder.exists()) {
				FileUtils.deleteDirectory(outFolder);
			}
		}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
	public void test() throws Exception {
		generateInput();
		
		// 1 - define tablespace
		TablespaceBuilder tablespaceBuilder = new TablespaceBuilder();
		
		TableBuilder paymentsBuilder = new TableBuilder(PAYMENTS_SCHEMA).addCSVTextFile(PAYMENTS_FILE + "");
		paymentsBuilder.partitionBy("name", "commerce");
		
		TableBuilder logsBuilder = new TableBuilder(LOGS_SCHEMA).addCSVTextFile(LOGS_FILE + "");
		logsBuilder.partitionBy("name", "commerce");
		
		tablespaceBuilder.add(paymentsBuilder.build());
		tablespaceBuilder.add(logsBuilder.build());
		
		tablespaceBuilder.setNPartitions(3);
		
		TablespaceSpec tablespace = tablespaceBuilder.build();
		
		// 2 - generate view
		Path outputPath = new Path(TEST_OUTPUT);
		TablespaceGenerator viewGenerator = new TablespaceGenerator(tablespace, outputPath, this.getClass());
		viewGenerator.generateView(new Configuration(), SamplingType.FULL_SCAN, new TupleSampler.RandomSamplingOptions());
		
		// 3 - assert result
		List totalPayments = new ArrayList();
		List totalLogs = new ArrayList();
		List distinctKeys = new ArrayList();
		
		for(int i = 0; i < 3; i++) {
			SQLite4JavaClient manager = new SQLite4JavaClient(TEST_OUTPUT + "/store/" + i + ".db", null);
			List list;
			list = JSONSerDe.deSer(manager.query("SELECT * FROM payments;", 100), ArrayList.class);
			totalPayments.addAll(list);
			Set<String> uniqueKeysInPartition = new HashSet<String>();
			for(Object obj: list) {
				uniqueKeysInPartition.add(((Map)obj).get("name") + "" + ((Map)obj).get("commerce"));
			}
			distinctKeys.addAll(uniqueKeysInPartition);
			list = JSONSerDe.deSer(manager.query("SELECT * FROM logs;", 100), ArrayList.class);
			totalLogs.addAll(list);
			manager.close();
		}

		assertEquals(4, distinctKeys.size());
		assertTrue(distinctKeys.contains("Tanos1"));
		assertTrue(distinctKeys.contains("Iván1"));
		assertTrue(distinctKeys.contains("Iván2"));
		assertTrue(distinctKeys.contains("Pere3"));
		assertEquals(5, totalLogs.size());
		assertEquals(7, totalPayments.size());
	}
	
	private void generateInput() throws IOException {
		File folder = new File(TEST_INPUT);
		folder.mkdir();
		
		generateInputPayments();
		generateInputLogs();
	}
	
	private void generateInputPayments() throws IOException {
		String paymentData = "";
		paymentData += "Tanos" + TAB + "1" + TAB + "350" + TAB + "EUR" + "\n";
		paymentData += "Tanos" + TAB + "1" + TAB + "370" + TAB + "EUR" + "\n";
		paymentData += "Iván" + TAB + "1" + TAB + "250" + TAB + "EUR" + "\n";
		paymentData += "Iván" + TAB + "1" + TAB + "150" + TAB + "EUR" + "\n";
		paymentData += "Iván" + TAB + "2" + TAB + "250" + TAB + "EUR" + "\n";
		paymentData += "Iván" + TAB + "2" + TAB + "150" + TAB + "EUR" + "\n";
		paymentData += "Pere" + TAB + "3" + TAB + "550" + TAB + "EUR" + "\n";
		
		Files.write(paymentData, PAYMENTS_FILE, Charset.defaultCharset());
	}
	
	private void generateInputLogs() throws IOException {
		String logData = "";
		logData += "Tanos" + TAB + "1" + TAB +"10:30pm" + TAB + "UP" + TAB + "Greece" + "\n"; 
		logData += "Tanos" + TAB + "1" + TAB + "10:32pm" + TAB + "DOWN" + TAB + "Spain" + "\n";
		logData += "Iván" + TAB + "1" + TAB + "08:30pm" + TAB + "UP" + TAB + "Greece" + "\n"; 
		logData += "Iván" + TAB + "2" + TAB + "08:32pm" + TAB + "UP" + TAB + "Germany" + "\n";
		logData += "Pere" + TAB + "3" + TAB + "06:30pm" + TAB + "DOWN" + TAB + "Spain" + "\n"; 
		
		Files.write(logData, LOGS_FILE, Charset.defaultCharset());
	}	
}