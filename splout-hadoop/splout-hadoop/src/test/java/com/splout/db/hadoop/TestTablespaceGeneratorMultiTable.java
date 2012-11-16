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
import com.splout.db.common.SQLiteJDBCManager;
import com.splout.db.hadoop.TupleSampler.SamplingType;

public class TestTablespaceGeneratorMultiTable {

	public static String TEST_INPUT  = "in-" + TestTablespaceGeneratorMultiTable.class.getName();
	public static String TEST_OUTPUT = "out-" + TestTablespaceGeneratorMultiTable.class.getName();
	public static Character TAB = '\t';
	
	// ---- //
	public static Schema PAYMENTS_SCHEMA = new Schema("payments", Fields.parse("name:string, amount:int, currency:string"));
	public static Schema LOGS_SCHEMA = new Schema("logs", Fields.parse("name:string, date:string, action:string, loc:string"));
	public static Schema GEODATA_SCHEMA = new Schema("geodata", Fields.parse("loc:string, lat:double, lng: double"));
	
	// ---- //
	public static File PAYMENTS_FILE = new File(TEST_INPUT, "payments.txt");
	public static File LOGS_FILE = new File(TEST_INPUT, "logs.txt");
	public static File GEODATA_FILE = new File(TEST_INPUT, "geodata.txt");

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
		paymentsBuilder.partitionBy("name");
		
		TableBuilder logsBuilder = new TableBuilder(LOGS_SCHEMA).addCSVTextFile(LOGS_FILE + "");
		logsBuilder.partitionBy("name");
		
		TableBuilder geoData = new TableBuilder(GEODATA_SCHEMA).addCSVTextFile(GEODATA_FILE + "");
		geoData.replicateToAll();
		
		tablespaceBuilder.add(paymentsBuilder.build());
		tablespaceBuilder.add(logsBuilder.build());
		tablespaceBuilder.add(geoData.build());
		
		tablespaceBuilder.setNPartitions(3); // create 3 partitions: one for Iván, one for Tanos and one for Pere
		
		TablespaceSpec tablespace = tablespaceBuilder.build();
		
		// 2 - generate view
		Path outputPath = new Path(TEST_OUTPUT);
		TablespaceGenerator viewGenerator = new TablespaceGenerator(tablespace, outputPath);
		viewGenerator.generateView(new Configuration(), SamplingType.DEFAULT, new TupleSampler.DefaultSamplingOptions());
		
		// 3 - assert result
		List totalPayments = new ArrayList();
		List totalLogs = new ArrayList();
		List<String> jsonGeoData = new ArrayList<String>();
		
		for(int i = 0; i < 3; i++) {
			SQLiteJDBCManager manager = new SQLiteJDBCManager(TEST_OUTPUT + "/store/" + i + ".db", 10);
			List list;
			list = JSONSerDe.deSer(manager.query("SELECT * FROM payments;", 100), ArrayList.class);
			totalPayments.addAll(list);
			assertEquals(1, list.size()); // There is one payment per each person
			list = JSONSerDe.deSer(manager.query("SELECT * FROM logs;", 100), ArrayList.class);
			totalLogs.addAll(list);
			assertEquals(2, list.size()); // There are two log events per each person
			String geoDataStr = manager.query("SELECT * FROM geodata;", 100);
			jsonGeoData.add(geoDataStr);
			list = JSONSerDe.deSer(geoDataStr, ArrayList.class);
			assertEquals(3, list.size()); // There are 3 countries in the replicated geo data file
			manager.close();
		}
		
		assertEquals(6, totalLogs.size());
		assertEquals(3, totalPayments.size());
		assertEquals(3, jsonGeoData.size());
		assertEquals(jsonGeoData.get(0), jsonGeoData.get(1));
		assertEquals(jsonGeoData.get(0), jsonGeoData.get(2));
		
		Set<String> seenNames = new HashSet<String>();
		for(Object log: totalLogs) {
			Map<String, Object> obj = (Map<String, Object>)log;
			seenNames.add((String) obj.get("name"));
		}		
		assertEquals(3, seenNames.size());
		
		seenNames = new HashSet<String>();
		for(Object log: totalPayments) {
			Map<String, Object> obj = (Map<String, Object>)log;
			seenNames.add((String) obj.get("name"));
		}
		assertEquals(3, seenNames.size());
	}
	
	private void generateInput() throws IOException {
		File folder = new File(TEST_INPUT);
		folder.mkdir();
		
		generateInputPayments();
		generateInputLogs();
		generateInputGeo();
	}
	
	private void generateInputPayments() throws IOException {
		String paymentData = "";
		paymentData += "Tanos" + TAB + "350" + TAB + "EUR" + "\n";
		paymentData += "Iván" + TAB + "250" + TAB + "EUR" + "\n";
		paymentData += "Pere" + TAB + "550" + TAB + "EUR" + "\n";
		
		Files.write(paymentData, PAYMENTS_FILE, Charset.defaultCharset());
	}
	
	private void generateInputLogs() throws IOException {
		String logData = "";
		logData += "Tanos" + TAB + "10:30pm" + TAB + "UP" + TAB + "Greece" + "\n"; 
		logData += "Tanos" + TAB + "10:32pm" + TAB + "DOWN" + TAB + "Spain" + "\n";
		logData += "Iván" + TAB + "08:30pm" + TAB + "UP" + TAB + "Greece" + "\n"; 
		logData += "Iván" + TAB + "08:32pm" + TAB + "UP" + TAB + "Germany" + "\n";
		logData += "Pere" + TAB + "06:30pm" + TAB + "DOWN" + TAB + "Spain" + "\n"; 
		logData += "Pere" + TAB + "06:32pm" + TAB + "DOWN" + TAB + "Germany" + "\n";
		
		Files.write(logData, LOGS_FILE, Charset.defaultCharset());
	}
	
	private void generateInputGeo() throws IOException {
		String geoData = "";
		geoData += "Greece"   + TAB + "40" + TAB + "42" + "\n";
		geoData += "Spain"    + TAB + "38" + TAB + "40" + "\n";
		geoData += "Germany"  + TAB + "36" + TAB + "38" + "\n";
		
		Files.write(geoData, GEODATA_FILE, Charset.defaultCharset());
	}
}
