package com.splout.db.hadoop;

/*
 * #%L
 * Splout SQL Hadoop library
 * %%
 * Copyright (C) 2012 - 2013 Datasalt Systems S.L.
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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SimpleGeneratorIntegrationTest {

	@After
	@Before
	public void cleanUp() throws InterruptedException, IOException {
		Runtime.getRuntime().exec("rm -rf out-cascading-logs").waitFor();
		Runtime.getRuntime().exec("rm -rf out-pig-wordcount").waitFor();
		Runtime.getRuntime().exec("rm -rf out-hashtags").waitFor();
	}
	
	@Test
	public void testCascading() throws Exception {
		List<String> args = new ArrayList<String>();
		args.add("-i"); // input
		args.add("src/test/resources/cascading-tuples.bin"); // binary tuple file
		args.add("-o"); // output
		args.add("out-cascading-logs");
		args.add("-pby");
		args.add("metric");
		args.add("-p");
		args.add("1");
		args.add("-t");
		args.add("cascading_logs");
		args.add("-tb");
		args.add("cascading_logs");
		args.add("-it");
		args.add("cascading");
		args.add("-cc");
		args.add("day,month,year,count,metric,value"); // cascading column names
		
		SimpleGeneratorCMD.main(args.toArray(new String[0]));
		
		assertTrue(new File("out-cascading-logs/cascading_logs/store", "0.db").exists());
		assertTrue(new File("out-cascading-logs/cascading_logs/store", "0.db").length() > 0);
	}
	
	@Test
	public void testTuple() throws Exception {
		List<String> args = new ArrayList<String>();
		args.add("-i"); // input
		args.add("src/test/resources/pig-wordcount.bin"); // binary tuple file
		args.add("-o"); // output
		args.add("out-pig-wordcount");
		args.add("-pby");
		args.add("word");
		args.add("-p");
		args.add("2");
		args.add("-t");
		args.add("pig_word_count");
		args.add("-tb");
		args.add("pig_word_count");
		args.add("-it");
		args.add("tuple");
		
		SimpleGeneratorCMD.main(args.toArray(new String[0]));
		
		assertTrue(new File("out-pig-wordcount/pig_word_count/store", "0.db").exists());
		assertTrue(new File("out-pig-wordcount/pig_word_count/store", "0.db").length() > 0);
		assertTrue(new File("out-pig-wordcount/pig_word_count/store", "1.db").exists());
		assertTrue(new File("out-pig-wordcount/pig_word_count/store", "1.db").length() > 0);
	}
	
	@Test
	public void testText() throws Exception {
		List<String> args = new ArrayList<String>();
		args.add("-i"); // input
		args.add("src/test/resources/hashtags_space.txt"); // text file
		args.add("-o"); // output
		args.add("out-hashtags");
		args.add("-pby");
		args.add("hashtag");
		args.add("-p");
		args.add("2");
		args.add("-t");
		args.add("hashtags");
		args.add("-tb");
		args.add("hashtags");
		// text is the default type so no need to add it
		// but we need to add the schema
		args.add("-s");
		args.add("ignore:string,date:string,count:long,hashtag:string");
		args.add("-sep");
		args.add(" ");
		
		SimpleGeneratorCMD.main(args.toArray(new String[0]));
		
		assertTrue(new File("out-hashtags/hashtags/store", "0.db").exists());
		assertTrue(new File("out-hashtags/hashtags/store", "0.db").length() > 0);
		assertTrue(new File("out-hashtags/hashtags/store", "1.db").exists());
		assertTrue(new File("out-hashtags/hashtags/store", "1.db").length() > 0);
	}
}
