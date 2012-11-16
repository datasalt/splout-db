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

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;

/**
 * A view over a {@link NullableTuple}. We override ITuple's get to use the wrapped NullabeTuple's getNullable.
 * This is useful for passing a ITuple instance to an API user that doesn't care about NullableTuples.
 * <p>
 * Note: This Tuple can't be serialized as it may return "null" in its get() methods.
 */
public class NullableTupleView implements ITuple {

	private NullableTuple wrappedTuple;
	
	public NullableTupleView(NullableTuple wrappedTuple) {
		setWrappedTuple(wrappedTuple);
	}
	
	public void setWrappedTuple(NullableTuple wrappedTuple) {
		this.wrappedTuple = wrappedTuple;
	}

	@Override
  public Schema getSchema() {
	  return wrappedTuple.getSchema();
  }
	@Override
  public void clear() {
		throw new IllegalAccessError("Can't clear a View Tuple");
	}
	@Override
  public Object get(int pos) {
	  return wrappedTuple.getNullable(pos);
  }
	@Override
  public Object get(String field) {
	  return wrappedTuple.getNullable(field);
  }
	@Override
  public void set(int pos, Object object) {
		wrappedTuple.set(pos, object);
	}
	@Override
  public void set(String field, Object object) {
		wrappedTuple.set(field, object);
	}
}
