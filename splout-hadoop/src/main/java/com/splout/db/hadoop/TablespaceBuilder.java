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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.datasalt.pangool.io.Schema.Field;
import com.splout.db.common.engine.Engine;

/**
 * This class is the main entry point for generating Splout views. Here we will use a Builder for adding the mapping
 * between a set of files in a FileSystem and the tables that we want to view in a Tablespace in Splout.
 * <p>
 * We need to use {@link TableBuilder} for obtaining {@link Table} beans. Then we can add Tables to the Tablespace.
 * <p>
 * We will obtain a {@link TablespaceSpec} bean after building it.
 */
// Still in development
public class TablespaceBuilder {

	// When not specifying a number of partitions, a table is partitioned using all the partitions of the Splout cluster
	public final static int ALL_PARTITIONS_AVAILABLE = -1;

	// In-memory incremental builder state
	private List<Table> partitionedTables = new ArrayList<Table>();
	private List<Table> replicatedTables = new ArrayList<Table>();
	
	// Init statements that will be executed when opening the database
	private String[] initStatements = null;
	
	// How to partition the Tablespace
	private int nPartitions = -1;
	
	private Engine engine = Engine.getDefault();

	/**
	 * Add a new table to the builder - use a string table identifier for this. The method will return a bean that we can
	 * use for filling the information of the table.
	 * 
	 * @throws TablespaceBuilderException
	 */
	public TablespaceBuilder add(Table table) throws TablespaceBuilderException {
		if(table.getTableSpec().getPartitionFields() == null
		    && table.getTableSpec().getPartitionByJavaScript() == null) {
			replicatedTables.add(table);
		} else {
			partitionedTables.add(table);
		}
		return this;
	}

	public void initStatements(String... initStatements) {
		this.initStatements = initStatements;
	}
	
	public void setNPartitions(int nPartitions) {
		this.nPartitions = nPartitions;
	}

	public void setEngine(Engine engine) {
		this.engine = engine;
	}
	
	/**
	 * Exception that is thrown if a Tablespace cannot be built because there is missing data or inconsistent data has
	 * been specified. The reason is specified as the message of the Exception.
	 */
	@SuppressWarnings("serial")
	public static class TablespaceBuilderException extends Exception {

		public TablespaceBuilderException(String msg) {
			super(msg);
		}
	}

	/**
	 * After specifying everything, call this method for building the final tablespace spec.
	 */
	public TablespaceSpec build() throws TablespaceBuilderException {
		if(nPartitions == -1) {
			throw new TablespaceBuilderException(
			    "Can't create a Tablespace with #partitions = -1. Please set #partitions.");
		}

		Field[] partitionFields = null;

		if(partitionedTables.size() == 0) {
			throw new TablespaceBuilderException(
			    "Can't create a Tablespace without any partitioned Table. At least one must be partitioned.");
		}

		Set<String> tableNames = new HashSet<String>();
		List<String> tableNameList = new ArrayList<String>();

		// Check that the partition field is coherent among different tables
		for(Table table : partitionedTables) {
			String tableName = table.getTableSpec().getSchema().getName();
			tableNames.add(tableName);
			tableNameList.add(tableName);

			Field[] thisPartitionFields = table.getTableSpec().getPartitionFields();
			if(partitionFields == null) {
				partitionFields = thisPartitionFields;
			} else {
				if(thisPartitionFields.length != partitionFields.length) {
					throw new TablespaceBuilderException("Different number of partition fields within tables: "
					    + thisPartitionFields.length + ", " + partitionFields.length
					    + " - must be the same for co-partitioning.");
				}
				for(int i = 0; i < thisPartitionFields.length; i++) {
					if(!thisPartitionFields[i].getType().equals(partitionFields[i].getType())) {
						throw new TablespaceBuilderException(
						    "Partition fields "
						        + thisPartitionFields[i]
						        + ", "
						        + partitionFields[i]
						        + " are not of the same type. They must be the same type for tables to be co-partitioned.");
					}
				}
			}
		}

		for(Table table : replicatedTables) {
			String tableName = table.getTableSpec().getSchema().getName();
			tableNames.add(tableName);
			tableNameList.add(tableName);
		}

		if(tableNames.size() != tableNameList.size()) {
			throw new TablespaceBuilderException(
			    "There is collision between table names. Maybe you added the same name twice. Table names: "
			        + tableNameList);
		}

		if(initStatements == null) {
			return new TablespaceSpec(partitionedTables, replicatedTables, nPartitions, null, engine);
		} else {
			return new TablespaceSpec(partitionedTables, replicatedTables, nPartitions, Arrays.asList(initStatements), engine);			
		}
	}
}
