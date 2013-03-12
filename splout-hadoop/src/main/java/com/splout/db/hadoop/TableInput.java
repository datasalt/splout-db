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

import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;

/**
 * A file (represented by a Hadoop Path) associated with an Pangool's InputFormat. TableFiles are used by {@link Table} instances
 * to represent a mapping between files and Tables.
 */
public class TableInput {

	private final InputFormat<ITuple, NullWritable> format;
	private final Path[] paths;
	private final Schema fileSchema;
	private final RecordProcessor recordProcessor;
	private final Map<String, String> specificHadoopInputFormatContext;
	
	TableInput(InputFormat<ITuple, NullWritable> format, Map<String, String> specificHadoopInputFormatContext, Schema fileSchema, RecordProcessor recordProcessor, Path... paths) {
		if(format == null) {
			throw new IllegalArgumentException("Input format can't be null");
		}
		if(fileSchema == null) {
			throw new IllegalArgumentException("File schema can't be null");
		}
		if(recordProcessor == null) {
			throw new IllegalArgumentException("Record processor can't be null. Use new IdentityRecordProcessor() for using the default one.");
		}
		if(paths == null) {
			throw new IllegalArgumentException("Input paths can't be null");
		}
		this.format = format;
		this.fileSchema = fileSchema;
		this.recordProcessor = recordProcessor;
		this.paths = paths;
		this.specificHadoopInputFormatContext = specificHadoopInputFormatContext;
	}

	public InputFormat<ITuple, NullWritable> getFormat() {
  	return format;
  }
	public Path[] getPaths() {
  	return paths;
  }
	public Schema getFileSchema() {
  	return fileSchema;
  }
	public RecordProcessor getRecordProcessor() {
  	return recordProcessor;
  }
	public Map<String, String> getSpecificHadoopInputFormatContext() {
  	return specificHadoopInputFormatContext;
  }
}
