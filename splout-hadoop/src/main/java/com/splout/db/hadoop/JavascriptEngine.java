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

import org.mozilla.javascript.Context;
import org.mozilla.javascript.Function;
import org.mozilla.javascript.Scriptable;

/**
 * An engine for embedding JavaScript into Java using Rhino, etc.
 */
public class JavascriptEngine {

  private Context cx;
  private Scriptable scope;

  public JavascriptEngine(String javascript) throws Throwable {
    try {
      cx = Context.enter();
      scope = cx.initStandardObjects();
      cx.evaluateString(scope, javascript, "<cmd>", 1, null);
    } catch (Throwable t) {
      // https://developer.mozilla.org/en-US/docs/Rhino/Scopes_and_Contexts
      Context.exit();
      cx = null;
      throw t;
    }
  }

  public String execute(String methodName, Object... params) throws Throwable {
    if (cx == null) {
      throw new IllegalStateException("Can't execute a function, context is null!");
    }
    Object fObj = scope.get(methodName, scope);
    Function f = (Function) fObj;
    return Context.toString(f.call(cx, scope, scope, params));
  }

  public void close() {
    if (cx != null) {
      Context.exit();
    }
  }
}
