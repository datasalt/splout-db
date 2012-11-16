package com.splout.db.examples;

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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.io.Files;

/**
 * A synthetic data generator for the Page Counts Example. A file with project names must be provided. Then, it produces page views
 * for each project for some days, filling all the hours and rolling per-hour files like in the original dataset.
 */
public class PageCountsDataGenerator {
	
	private final static DateTimeFormatter format = DateTimeFormat.forPattern("yyyyMMdd-HHmmss");
	
	public void generate(File namesFile, int days, File outFolder) throws IOException {
		if(outFolder.exists()) {
			FileUtils.deleteDirectory(outFolder);
		}
		outFolder.mkdirs();
		
		if(!namesFile.exists()) {
			throw new IllegalArgumentException("Provided names file doesn't exist (" + namesFile + ")");
		}
		
		List<String> names = Files.readLines(namesFile, Charset.forName("UTF-8"));
		
		DateTime today = new DateTime();
		DateTime someDaysBefore = today.minusDays(days);
		
		someDaysBefore = someDaysBefore.withMinuteOfHour(0);
		someDaysBefore = someDaysBefore.withSecondOfMinute(0);
		
		while(someDaysBefore.isBefore(today)) {
			for(int hour = 0; hour < 24; hour++) {
				
				someDaysBefore = someDaysBefore.withHourOfDay(hour);
				
				File currentFile = new File(outFolder, "pagecounts-" + format.print(someDaysBefore));
				BufferedWriter writer = new BufferedWriter(new FileWriter(currentFile));
				
				for(String name: names) {
					int pageviews = (int)(Math.random() * 10000) + 1;
					writer.write("en " + name + " " + pageviews + " 0" + "\n");
				}
				
				writer.close();
			}
			someDaysBefore = someDaysBefore.plusDays(1);
		}
	}
	
	public static void main(String[] args) throws IOException {
		PageCountsDataGenerator generator = new PageCountsDataGenerator();
		generator.generate(new File("/home/pere/pagecounts-s3/names.txt"), 5, new File("out-synthetic-pagecounts"));
	}
}
