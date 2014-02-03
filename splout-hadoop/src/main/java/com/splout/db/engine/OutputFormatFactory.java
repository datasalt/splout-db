package com.splout.db.engine;

/*
 * #%L
 * Splout SQL Hadoop library
 * %%
 * Copyright (C) 2012 - 2014 Datasalt Systems S.L.
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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.OutputFormat;

import com.datasalt.pangool.io.ITuple;
import com.splout.db.hadoop.TableSpec;
import com.splout.db.hadoop.TablespaceGenerator;
import com.splout.db.hadoop.engine.SploutSQLOutputFormat;
import com.splout.db.hadoop.engine.SploutSQLProxyOutputFormat;

/**
 * Stateless factory that should be used to provide an appropriate OutputFormat for generating a Tablespace with a certain
 * {@link SploutEngine}. Will be called by {@link TablespaceGenerator} before executing the generation job.
 * <p>
 * The contract of the OutputFormat is to produce a single "partition_id".db binary file with the contents that the engine
 * needs to act upon. If the engine needs multiple files, this file should be compressed for example with {@link CompressionUtil},
 * and decompressed in the "server" factory.
 */
public class OutputFormatFactory {

	@SuppressWarnings({ "unchecked", "rawtypes" })
  public static OutputFormat<ITuple, NullWritable> getOutputFormat(SploutEngine engine, int batchSize, TableSpec[] tbls) throws Exception {
		SploutSQLOutputFormat oF = null;
		
		Class cl;
		try {
			cl = Class.forName(engine.getOutputFormatClass());
		} catch(ClassNotFoundException e) {
			throw new IllegalArgumentException("Engine (" + engine.getId() + ") output format not in classpath: " + engine.getOutputFormatClass());
		}
		
		Class<SploutSQLOutputFormat> cl2;
		try {
			cl2 = cl.asSubclass(SploutSQLOutputFormat.class);
		} catch(ClassCastException e) {
			throw new IllegalArgumentException("Engine (" + engine.getId() + ") output format class not a subclass of " + SploutSQLOutputFormat.class + ": " + engine.getOutputFormatClass());
		}

		try {
			oF = cl2.getConstructor(Integer.class, TableSpec[].class).newInstance(batchSize, tbls);
		} catch(NoSuchMethodException e) {
			throw new IllegalArgumentException("Engine (" + engine.getId() + ") error instantiating " + engine.getOutputFormatClass());
		} catch(SecurityException e) {
			throw new IllegalArgumentException("Engine (" + engine.getId() + ") error instantiating " + engine.getOutputFormatClass());
		}
		
		return new SploutSQLProxyOutputFormat(oF);
	}
}
