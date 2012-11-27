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

import com.datasalt.pangool.io.TupleFile;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.junit.Before;
import org.junit.Test;

import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.tuplemr.mapred.lib.input.TupleInputFormat;
import com.datasalt.pangool.utils.AvroUtils;
import com.datasalt.pangool.utils.HadoopUtils;
import com.datasalt.pangool.utils.test.AbstractHadoopTestLibrary;
import com.splout.db.common.JSONSerDe;
import com.splout.db.common.PartitionMap;
import com.splout.db.hadoop.TupleSampler.SamplingType;

public class TestTablespaceGeneratorJavaScript extends AbstractHadoopTestLibrary {

	public final static String INPUT = "in-" + TestTablespaceGeneratorJavaScript.class.getName();
	public final static String OUTPUT = "out-" + TestTablespaceGeneratorJavaScript.class.getName();
	static Schema theSchema1 = new Schema("schema1", Fields.parse("id:string, value:string"));

	@Before
	public void test() {
		System.out.println(System.getProperty("java.library.path"));
	}
	
	@Test
	public void simpleTest() throws Exception {
		initHadoop();
		trash(INPUT, OUTPUT);

    TupleFile.Writer writer = new TupleFile.Writer(fS,  getConf(), new Path(INPUT), theSchema1);

		writer.append(TestTablespaceGenerator.getTuple("aa1", "value1"));
		writer.append(TestTablespaceGenerator.getTuple("aa2", "value2"));

		writer.append(TestTablespaceGenerator.getTuple("ab1", "value3"));
		writer.append(TestTablespaceGenerator.getTuple("ab2", "value4"));

		writer.append(TestTablespaceGenerator.getTuple("bb1", "value5"));
		writer.append(TestTablespaceGenerator.getTuple("bb2", "value6"));

		writer.close();

		TablespaceBuilder builder = new TablespaceBuilder();
		builder.setNPartitions(3);
		TableBuilder tableBuilder = new TableBuilder(theSchema1);
		tableBuilder.addFile(new TableInput(new TupleInputFormat(), theSchema1, new IdentityRecordProcessor(), new Path(INPUT)));
		// Partition by a javascript that returns the first two characters
		tableBuilder
		    .partitionByJavaScript("function partition(record) { var str = record.get('id').toString(); return str.substring(0, 2); }");
		builder.add(tableBuilder.build());

		TablespaceGenerator viewGenerator = new TablespaceGenerator(builder.build(), new Path(OUTPUT));
		viewGenerator.generateView(getConf(), SamplingType.DEFAULT, new TupleSampler.DefaultSamplingOptions());
		
		PartitionMap partitionMap = JSONSerDe.deSer(
		    HadoopUtils.fileToString(FileSystem.getLocal(getConf()), new Path(OUTPUT, "partition-map")), PartitionMap.class);
		
		assertEquals(null, partitionMap.getPartitionEntries().get(0).getMin());
		assertEquals("ab", partitionMap.getPartitionEntries().get(0).getMax());
		
		assertEquals("ab", partitionMap.getPartitionEntries().get(1).getMin());
		assertEquals("bb", partitionMap.getPartitionEntries().get(1).getMax());
		
		assertEquals("bb", partitionMap.getPartitionEntries().get(2).getMin());
		assertEquals(null, partitionMap.getPartitionEntries().get(2).getMax());

		trash(INPUT, OUTPUT);
	}
}
