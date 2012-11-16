package com.splout.db.common;

/*
 * #%L
 * Splout SQL commons
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

import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

/**
 * Helper class for serializing / deserializing from / to JSON using Jackson
 */
public class JSONSerDe {

	private static ObjectMapper mapper = new ObjectMapper();

	@SuppressWarnings("serial")
	public static class JSONSerDeException extends Exception {

		public JSONSerDeException(Exception excp) {
			super(excp);
		}
	}

	public static String ser(Object obj) throws JSONSerDeException {
		try {
			return mapper.writeValueAsString(obj);
		} catch(JsonGenerationException e) {
			throw new JSONSerDeException(e);
		} catch(JsonMappingException e) {
			throw new JSONSerDeException(e);
		} catch(IOException e) {
			throw new JSONSerDeException(e);
		}
	}

	@SuppressWarnings("unchecked")
  public static <T> T deSer(String str, TypeReference<T> ref) throws JSONSerDeException {
		try {
			return (T) mapper.readValue(str, ref);
		} catch(JsonParseException e) {
			throw new JSONSerDeException(e);
		} catch(JsonMappingException e) {
			throw new JSONSerDeException(e);
		} catch(IOException e) {
			throw new JSONSerDeException(e);
		}
	}

	public static <T> T deSer(String str, Class<T> clazz) throws JSONSerDeException {
		try {
			return (T) mapper.readValue(str, clazz);
		} catch(JsonParseException e) {
			throw new JSONSerDeException(e);
		} catch(JsonMappingException e) {
			throw new JSONSerDeException(e);
		} catch(IOException e) {
			throw new JSONSerDeException(e);
		}
	}
}
