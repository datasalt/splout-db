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

import java.io.Serializable;
import java.util.Arrays;

import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;

/**
 * Simple immutable bean that specifies the Pangool Schema of a Splout Table and the Fields that need to be indexed and how it is partitioned.
 * It is part of a {@link Table} bean. It is also used by {@link TupleSQLiteOutputFormat}.
 */
@SuppressWarnings("serial")
public class TableSpec implements Serializable {

	private final Schema schema;
	private final Field[] partitionFields;
	private final FieldIndex[] indexes;
	private final String partitionByJavaScript;
	private final String[] postSQL;
	private final String[] preSQL;
	
	public TableSpec(Schema schema, Field partitionField) {
		this(schema, new Field[] { partitionField }, new FieldIndex[] { new FieldIndex(partitionField) }, null, null);
	}
	
	public TableSpec(Schema schema, Field[] partitionFields, FieldIndex[] indexes, String[] preSQL, String[] postSQL) {
		this.schema = schema;
		this.partitionFields = partitionFields;
		this.indexes = indexes;
		this.partitionByJavaScript = null;
		this.preSQL = preSQL;
		this.postSQL = postSQL;
	}

	public TableSpec(Schema schema, String partitionByJavaScript, FieldIndex[] indexes, String[] preSQL, String[] postSQL) {
		this.schema = schema;
		this.partitionFields = null;
		this.partitionByJavaScript = partitionByJavaScript;
		this.indexes = indexes;
		this.preSQL = preSQL;
		this.postSQL = postSQL;
	}
	
	/**
	 * A database index made up by one or more Pangool Fields.
	 */
	public static class FieldIndex implements Serializable {
		
		private Field[] fields;
		
		public FieldIndex(Field field) {
			this.fields = new Field[] { field };
		}
		
		public FieldIndex(Field... fields) {
			this.fields = fields;
		}
		
		public Field[] getIndexFields() {
			return fields;
		}
		
		@Override
		public String toString() {
		  return Arrays.toString(fields);
		}
	}
	
	// ---- Getters & setters ---- //

	public Schema getSchema() {
		return schema;
	}
	public FieldIndex[] getIndexes() {
  	return indexes;
  }
	public Field[] getPartitionFields() {
  	return partitionFields;
  }
	public String getPartitionByJavaScript() {
  	return partitionByJavaScript;
  }
	public String[] getPostSQL() {
	  return postSQL;
  }
	public String[] getPreSQL() {
	  return preSQL;
  }
}