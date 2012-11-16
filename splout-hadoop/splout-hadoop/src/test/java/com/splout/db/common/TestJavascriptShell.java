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

import javax.script.Bindings;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.junit.Ignore;
import org.junit.Test;

import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;

public class TestJavascriptShell {

	@Test
	@Ignore
	public void test() throws ScriptException, NoSuchMethodException {
		ScriptEngineManager manager = new ScriptEngineManager();
		ScriptEngine engine = manager.getEngineByName("js");
		Bindings bindings = engine.createBindings();
		
		Tuple tuple = new Tuple(new Schema("schema", Fields.parse("foo:string, bar:int")));
		tuple.set("foo", "foo1");
		tuple.set("bar", 10);
		bindings.put("tuple", tuple);
		
		String script = "function fooFunction() { return tuple.get('foo'); }";
		Invocable invocableEngine = (Invocable) engine;
		engine.eval(script, bindings);
		invocableEngine.invokeFunction("fooFunction");
	}
}
