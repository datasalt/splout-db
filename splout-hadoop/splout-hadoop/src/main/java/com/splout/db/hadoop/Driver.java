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

import org.apache.hadoop.util.ProgramDriver;

import com.splout.db.benchmark.BenchmarkStoreTool;
import com.splout.db.examples.PageCountsExample;
import com.splout.db.integration.HadoopIntegrationTest;
import com.splout.db.integration.TestDemo;

/**
 * Hadoop's Driver - add here any Hadoop programs that could be run by default with the Splout JAR.
 */
public class Driver extends ProgramDriver {

	public Driver() throws Throwable {
		super();
		addClass("datagenerator", TestDemo.class, "A simple data generator that creates a Splout view and deploys it to localhost. For preliminar testing.");
		addClass("benchmarkstoretool", BenchmarkStoreTool.class, "A tool for creating a tablespace for benchmarking Splout.");
		addClass("benchmarkdeploytool", DeployerCMD.class, "A tool for deploying a tablespace created with Benchmark store tool.");
		addClass("generate", GeneratorCMD.class, "A tool for generating tablespaces from existing files (CSV).");
		addClass("simple-generate", SimpleGeneratorCMD.class, "A tool for converting a CSV file into a tablespace with just one table. See <generate> tool for multiple table or multiple tablespace cases.");
		addClass("deploy", DeployerCMD.class, "A tool for deploying tablespaces generated with tools like <generate> or <simple-generate> into an existing Splout cluster");
		addClass("pagecounts", PageCountsExample.class, "The Wikipedia Page Counts Example");
		addClass("integrationtest", HadoopIntegrationTest.class, "A Hadoop-compatibility integrationt test");
	}
	
	public static void main(String[] args) throws Throwable {
		Driver driver = new Driver();
		driver.driver(args);
		System.exit(0);
	}
}
