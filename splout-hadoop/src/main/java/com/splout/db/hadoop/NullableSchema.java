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

import com.datasalt.pangool.io.Schema;

/**
 * An extended Pangool (http://pangool.net) Schema that adds one more field for indicating which fields are "null".
 * This is part of a workaround for being able to serialize null values using Pangool (see {@link NullableTuple}.
 */
@SuppressWarnings("serial")
public class NullableSchema extends Schema {

	public final static String NULLS_FIELD = "_nulls";
	
	public NullableSchema(Schema schema) {
	  super(schema.getName(), addNulls(schema));
  }
	
	public static boolean isNullable(Schema schema) {
		for(Field field: schema.getFields()) {
			if(field.getName().equals(NULLS_FIELD)) {
				// Already a nullable schema
			return true;
			}
		}	
		return false;
	}
	
	public static List<Field> addNulls(Schema schema) {
		if(isNullable(schema)) {
			return schema.getFields();
		}
		List<Field> newFields = new ArrayList<Field>();
		newFields.addAll(schema.getFields());
		newFields.add(Field.create(NULLS_FIELD, Field.Type.BYTES));
		return newFields;
	}
}
