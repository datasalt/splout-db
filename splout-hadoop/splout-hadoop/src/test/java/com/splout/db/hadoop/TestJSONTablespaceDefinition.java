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

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import org.junit.Test;

import com.google.common.io.Files;
import com.splout.db.common.JSONSerDe;
import com.splout.db.common.JSONSerDe.JSONSerDeException;
import com.splout.db.hadoop.TableBuilder.TableBuilderException;
import com.splout.db.hadoop.TablespaceBuilder.TablespaceBuilderException;

public class TestJSONTablespaceDefinition {

	@Test
	public void test() throws IOException, JSONSerDeException, TablespaceBuilderException, TableBuilderException {
		String JSON_TO_TEST = Files.toString(new File(this.getClass().getClassLoader().getResource("tablespace.json").getFile()), Charset.forName("UTF-8"));
		JSONTablespaceDefinition def = JSONSerDe.deSer(JSON_TO_TEST, JSONTablespaceDefinition.class);
		def.build();
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testInvalidTablespace() throws IOException, JSONSerDeException, TablespaceBuilderException, TableBuilderException {
		String JSON_TO_TEST = Files.toString(new File(this.getClass().getClassLoader().getResource("invalidtablespace.json").getFile()), Charset.forName("UTF-8"));
		JSONTablespaceDefinition def = JSONSerDe.deSer(JSON_TO_TEST, JSONTablespaceDefinition.class);
		def.build();
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testInvalidTable() throws IOException, JSONSerDeException, TablespaceBuilderException, TableBuilderException {
		String JSON_TO_TEST = Files.toString(new File(this.getClass().getClassLoader().getResource("invalidtable.json").getFile()), Charset.forName("UTF-8"));
		JSONTablespaceDefinition def = JSONSerDe.deSer(JSON_TO_TEST, JSONTablespaceDefinition.class);
		def.build();
	}
}
