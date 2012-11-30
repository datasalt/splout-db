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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.google.common.collect.ImmutableList;
import com.splout.db.hadoop.TableSpec.FieldIndex;

/**
 * Immutable bean that defines a Tablespace whose view has to be generated. It may contain one or more {@link Table} beans.
 * It can be obtained by {@link TablespaceBuilder}.
 */
public class TablespaceSpec {

	private final ImmutableList<Table> partitionedTables;
	private final ImmutableList<Table> replicateAllTables;
	private final int nPartitions;
	private final List<String> initStatements;

	TablespaceSpec(List<Table> partitionedTables, List<Table> replicateAllTables, int nPartitions, List<String> initStatements) {
		this.partitionedTables = ImmutableList.copyOf(partitionedTables);
		this.replicateAllTables = ImmutableList.copyOf(replicateAllTables == null ? new ArrayList<Table>() : replicateAllTables);
		this.nPartitions = nPartitions;
		this.initStatements = initStatements;
	}

	/**
	 * (Common case that can be built without using the builder)
	 */
	public static TablespaceSpec of(Schema schema, String partitionField, Path input, InputFormat<ITuple, NullWritable> inputFormat, int nPartitions) {
		return of(schema, new String[] {  partitionField } , input, inputFormat, nPartitions);
	}
	
	public static TablespaceSpec of(Schema schema, String[] partitionFields, Path input, InputFormat<ITuple, NullWritable> inputFormat, int nPartitions) {
		List<Table> partitionedTables = new ArrayList<Table>();
		if(schema == null) {
			throw new IllegalArgumentException("Schema can't be null.");
		}
		if(partitionFields == null) {
			throw new IllegalArgumentException("Partition fields can't be null");
		}
		if(input == null) {
			throw new IllegalArgumentException("Input path can't be null");
		}
		if(inputFormat == null) {
			throw new IllegalArgumentException("Input format can't be null");
		}
		List<Field> fields = new ArrayList<Field>();
		for(String partitionField: partitionFields) {
			Field field = schema.getField(partitionField);
			if(field == null) {
				throw new IllegalArgumentException("Partition field not contained in input schema: " + partitionField);
			}
			fields.add(field);
		}
		Field[] partitionByFields = fields.toArray(new Field[0]);
		partitionedTables.add(new Table(new TableInput(inputFormat, schema, new IdentityRecordProcessor(), input), new TableSpec(schema, partitionByFields, new FieldIndex[] { new FieldIndex(partitionByFields) },null, null, null, null, null)));
		TablespaceSpec tablespace = new TablespaceSpec(partitionedTables, new ArrayList<Table>(), nPartitions, null);
		return tablespace;
	}
	
	// ---- Getters ---- //
	
	public ImmutableList<Table> getPartitionedTables() {
  	return partitionedTables;
  }
	public ImmutableList<Table> getReplicateAllTables() {
  	return replicateAllTables;
  }
	public int getnPartitions() {
  	return nPartitions;
  }
	public List<String> getInitStatements() {
  	return initStatements;
  }
}
