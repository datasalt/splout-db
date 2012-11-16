package com.splout.db.common;

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

import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;

/**
 * Testing helper class.
 */
public class SploutHadoopTestUtils {

	/**
	 * A simple Pangool Schema called "tablespace1" made up by a string and an integer
	 */
	public final static Schema SCHEMA = new Schema("tablespace1", Fields.parse("id:string, value:int"));
	
	/**
	 * Returns a Tuple conforming to a simple schema: {@link #SCHEMA}.
	 */
	public static ITuple getTuple(String id, int value) {
		ITuple tuple = new Tuple(SCHEMA);
		tuple.set("id", id);
		tuple.set("value", value);
		return tuple;
	}
}
