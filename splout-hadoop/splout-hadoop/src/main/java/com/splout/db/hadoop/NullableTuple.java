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

import java.io.PrintStream;
import java.nio.ByteBuffer;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.io.Tuple;

/**
 * A wrapper over a Pangool (http://pangool.net) Tuple for being able to serialize null values. Because Pangool defines
 * an intermediate Hadoop serialization, it doesn't support serializing nulls. We add a Tuple on top of Pangool's Tuple
 * that supports serializing null values and only handles primitive types (not OBJECT or ENUM). It can't be used as an
 * intermediate Tuple in Pangool but it can otherwise be serialized as a Tuple Field inside another Tuple, or persisted
 * using a normal TupleRecordWriter and a corresponding {@link NullableSchema}. NullableTuple also adds deep-copy
 * semantics to the Pangool Tuple by restricting its usage to primitive types. Deep-copy can be enabled by a flag in
 * appropriate methods.
 * <p>
 * A NullableTuple adds one more Field to the wrapped Tuple (see {@link NullableSchema}. This new fields indicate which
 * fields are null. The fields that are null contain a non-null value (0 for Integers, for instance) so that they can be
 * serialized. The overrided get() method returns those values so that Pangool can serialize the Tuple, however, for
 * null-aware getter one must use {@link #getNullable(int)} or {@link #getNullable(String)}. For conveniece, it is also
 * possible to create a non-serializable view whose get() will return nulls by using {@link NullableTupleView}.
 */
@SuppressWarnings("serial")
public class NullableTuple extends Tuple {

	private boolean wrappedIsNullable = false;
	private int nFields;

	public NullableTuple(ITuple tuple) {
		this(tuple, false);
	}

	public NullableTuple(ITuple tuple, boolean deepCopy) {
		this(tuple.getSchema());
		setWrappedTuple(tuple);
	}

	/**
	 * For instance reusing, instantiate the Tuple once with the wrapped Schema and use {@link #setWrappedTuple(ITuple)}
	 * afterwards.
	 */
	public NullableTuple(Schema schema) {
		super(new NullableSchema(schema));
		if(NullableSchema.isNullable(schema)) {
			wrappedIsNullable = true;
			nFields = schema.getFields().size() - 1;
		} else {
			nFields = schema.getFields().size();
		}
		initNulls();
	}

	public void setWrappedTuple(ITuple wrappedTuple) {
		setWrappedTuple(wrappedTuple, false);
	}

	public void setWrappedTuple(ITuple wrappedTuple, boolean deepCopy) {
		clear();
		if(wrappedIsNullable && !(wrappedTuple instanceof NullableTupleView)) {
			super.set(nFields, wrappedTuple.get(NullableSchema.NULLS_FIELD));
			// Shallow copy the values
			for(int i = 0; i < nFields; i++) {
				super.set(i, wrappedTuple.get(i));
			}
		} else {
			// Do a first round of checking whether there are nulls or not
			for(int i = 0; i < nFields; i++) {
				Object obj = wrappedTuple.get(i);
				Field field = wrappedTuple.getSchema().getField(i);
				if(deepCopy && field.getType().equals(Type.STRING)) {
					// for deep-copying primitive types we have to take
					// into account that Strings are often wrapped into UTF8 objects in Pangool.
					// Other than that, it is safe to shallow-copy primitive types as a deep copy.
					// Usually you don't want to deep-copy unless you are keeping Tuples in an in-memory array.
					obj = obj.toString();
				}
				set(field.getName(), obj);
			}
		}
	}

	@Override
	public void clear() {
		for(int i = 0; i < nFields; i++) {
			super.set(i, null);
		}
		byte[] nulls = getNulls();
		if(nulls == null) {
			initNulls();
		} else {
			for(int i = 0; i < nulls.length; i++) {
				nulls[i] = 0;
			}
		}
	}

	public Object getNullable(int pos) {
		if(isNull(pos)) {
			return null;
		}
		return super.get(pos);
	}

	public Object getNullable(String field) {
		int pos = getSchema().getFieldPos(field);
		return getNullable(pos);
	}

	@Override
	public void set(int pos, Object object) {
		if(object != null) {
			setNoNull(pos);
		} else {
			setNull(pos);

			Field.Type type = getSchema().getField(pos).getType();
			switch(type) {

			case INT:
				object = 0;
				break;
			case BOOLEAN:
				object = false;
				break;
			case BYTES:
				object = new byte[0];
				break;
			case DOUBLE:
				object = 0d;
				break;
			case FLOAT:
				object = 0f;
				break;
			case LONG:
				object = 0l;
				break;
			case STRING:
				object = "";
				break;
			case ENUM:
				throw new RuntimeException("Unsupported operation: setting a null Enum");
			case OBJECT:
				throw new RuntimeException("Unsupported operation: setting a null Object");
			}
		}
		super.set(pos, object);
	}

	@Override
	public void set(String field, Object object) {
		int pos = getSchema().getFieldPos(field);
		set(pos, object);
	}

	// --- Helper methods for dealing with the byte[] bit set --- //

	public void initNulls() {
		byte[] nulls = new byte[(nFields / 8) + 1];
		for(int i = 0; i < nulls.length; i++) {
			nulls[i] = 0;
		}
		super.set(nFields, nulls);
	}

	public byte[] getNulls() {
		Object b = super.get(nFields);
		if(b instanceof byte[]) {
			return (byte[]) b;
		} else if(b instanceof ByteBuffer) {
			byte[] nullBytes = new byte[(nFields / 8) + 1];
			ByteBuffer bB = (ByteBuffer) b;
			int position = bB.position();
			bB.get(nullBytes, position, nullBytes.length);
			bB.position(position);
			// Replace deserialized ByteBuffer by the native byte array
			// This eases the possible modifications on the byte array even though it is less efficient than keeping the
			// bytebuffer
			super.set(nFields, nullBytes);
			return nullBytes;
		} else {
			throw new RuntimeException("Field that contains null info is not byte[] neither ByteBuffer!");
		}
	}

	public static boolean isNull(int pos, byte[] nulls, int offset) {
		return (nulls[offset + (pos / 8)] & (1 << (pos % 8))) > 0;		
	}
	
	public boolean isNull(int pos) {
		return isNull(pos, getNulls(), 0);
	}

	public void setNull(int pos) {
		byte[] nulls = getNulls();
		byte b = nulls[pos / 8];
		nulls[pos / 8] = (byte) (b | (1 << (pos % 8)));
	}

	public void setNoNull(int pos) {
		byte[] nulls = getNulls();
		byte b = nulls[pos / 8];
		nulls[pos / 8] = (byte) (b & (byte) (255 ^ (1 << (pos % 8))));
	}

	public void printNulls(PrintStream printStream) {
		byte[] nulls = getNulls();
		for(byte b : nulls) {
			printUnsignedByte(b, printStream);
		}
	}

	public static void printUnsignedByte(byte b, PrintStream printStream) {
		for(int i = 0; i < 8; i++) {
			printStream.print(((b & (1 << i)) > 0) ? "1" : "0");
		}
		printStream.println();
	}
}
