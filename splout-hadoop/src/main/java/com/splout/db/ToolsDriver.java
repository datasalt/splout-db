package com.splout.db;

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

import com.datasalt.pangool.PangoolDriver;
import com.splout.db.benchmark.BenchmarkStoreTool;
import com.splout.db.benchmark.BenchmarkTool;
import com.splout.db.hadoop.DeployerCMD;

/**
 * Driver to run different Splout tools
 * TODO Merge with the other Driver?
 */
public class ToolsDriver extends PangoolDriver {

	public ToolsDriver() throws Throwable {
		addClass("benchmark-store", BenchmarkStoreTool.class, "Creates a dataset to be used for benchmarking");
		addClass("benchmark-deploy", DeployerCMD.class, "Deploys a dataset created with the benchmar-store tool into a Splout cluster");
		addClass("benchmark", BenchmarkTool.class, "Runs a benchmark. benchmark-store and benchmark-deploy tool should have been run before.");
	}
	
	public static void main(String args[]) throws Throwable {
		new ToolsDriver().driver(args);
	}
}
