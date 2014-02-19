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

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.tuplemr.OrderBy;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleReducer;
import com.google.common.io.Files;
import com.splout.db.hadoop.TupleSampler.SamplingType;

@SuppressWarnings("serial")
public class TestTablespaceGeneratorTestInsertionSortOrder implements Serializable {

	public static String TEST_INPUT = "in-" + TestTablespaceGeneratorMultiTable.class.getName();
	public static String TEST_OUTPUT = "out-" + TestTablespaceGeneratorMultiTable.class.getName();
	public static Character TAB = '\t';

	// ---- //
	public static Schema PAYMENTS_SCHEMA = new Schema("payments",
	    Fields.parse("name:string, amount:int, currency:string"));
	public static Schema LOGS_SCHEMA = new Schema("logs",
	    Fields.parse("name:string, date:string, action:string, loc:string"));
	public static Schema GEODATA_SCHEMA = new Schema("geodata",
	    Fields.parse("loc:string, lat:double, lng: double"));

	// ---- //
	public static File PAYMENTS_FILE = new File(TEST_INPUT, "payments.txt");
	public static File LOGS_FILE = new File(TEST_INPUT, "logs.txt");
	public static File GEODATA_FILE = new File(TEST_INPUT, "geodata.txt");

	@Before
	@After
	public void cleanUp() throws IOException {
		for(String cleanUpFolder : new String[] { TEST_INPUT, TEST_OUTPUT }) {
			File outFolder = new File(cleanUpFolder);
			if(outFolder.exists()) {
				FileUtils.deleteDirectory(outFolder);
			}
		}
	}

	@Test
	public void test() throws Exception {
		generateInput();

		// 1 - define tablespace
		TablespaceBuilder tablespaceBuilder = new TablespaceBuilder();

		TableBuilder paymentsBuilder = new TableBuilder(PAYMENTS_SCHEMA).addCSVTextFile(PAYMENTS_FILE + "");
		paymentsBuilder.partitionBy("name");
		// Insert sort order:
		paymentsBuilder.insertionSortOrder(OrderBy.parse("name:desc"));

		TableBuilder logsBuilder = new TableBuilder(LOGS_SCHEMA).addCSVTextFile(LOGS_FILE + "");
		logsBuilder.partitionBy("name");
		// Insert sort order:
		logsBuilder.insertionSortOrder(OrderBy.parse("action:asc, loc:asc"));

		TableBuilder geoData = new TableBuilder(GEODATA_SCHEMA).addCSVTextFile(GEODATA_FILE + "");
		geoData.replicateToAll();
		// Insert sort order:
		geoData.insertionSortOrder(OrderBy.parse("loc:desc"));

		tablespaceBuilder.add(paymentsBuilder.build());
		tablespaceBuilder.add(logsBuilder.build());
		tablespaceBuilder.add(geoData.build());

		tablespaceBuilder.setNPartitions(1); // only one partition so we can check the order

		TablespaceSpec tablespace = tablespaceBuilder.build();

		// 2 - generate view
		Path outputPath = new Path(TEST_OUTPUT);
		TablespaceGenerator viewGenerator = new TablespaceGenerator(tablespace, outputPath, this.getClass());
		viewGenerator.setCustomReducer(new TupleReducer<ITuple, NullWritable>() {

			public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context,
			    Collector collector) throws IOException, InterruptedException, TupleMRException {
				
				Iterator<ITuple> it = tuples.iterator();
				
				// First table in schema order is payments, sorted by name desc
				
				Assert.assertEquals("Tanos", it.next().get("name").toString());
				Assert.assertEquals("Pere", it.next().get("name").toString());
				Assert.assertEquals("Iván", it.next().get("name").toString());
				
				// Second table in schema order is logs, sorted by (action asc, loc asc)
				
				ITuple next = it.next();
				Assert.assertEquals("DOWN", next.get("action").toString());
				Assert.assertEquals("Germany", next.get("loc").toString());
				next = it.next();
				Assert.assertEquals("DOWN", next.get("action").toString());
				Assert.assertEquals("Spain", next.get("loc").toString());
				next = it.next();
				Assert.assertEquals("DOWN", next.get("action").toString());
				Assert.assertEquals("Spain", next.get("loc").toString());
				next = it.next();
				Assert.assertEquals("UP", next.get("action").toString());
				Assert.assertEquals("Germany", next.get("loc").toString());
				next = it.next();
				Assert.assertEquals("UP", next.get("action").toString());
				Assert.assertEquals("Greece", next.get("loc").toString());
				next = it.next();
				Assert.assertEquals("UP", next.get("action").toString());
				Assert.assertEquals("Greece", next.get("loc").toString());
				
				// Thrid table in schema order is geodata, sorted by loc desc

				Assert.assertEquals("Spain", it.next().get("loc").toString());
				Assert.assertEquals("Greece", it.next().get("loc").toString());
				Assert.assertEquals("Germany", it.next().get("loc").toString());
			};
		});
		
		viewGenerator.generateView(new Configuration(), SamplingType.RANDOM,
		    new TupleSampler.RandomSamplingOptions());
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
		geoData += "Greece" + TAB + "40" + TAB + "42" + "\n";
		geoData += "Spain" + TAB + "38" + TAB + "40" + "\n";
		geoData += "Germany" + TAB + "36" + TAB + "38" + "\n";

		Files.write(geoData, GEODATA_FILE, Charset.defaultCharset());
	}
}
