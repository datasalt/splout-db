package com.splout.db.hazelcast;

/*
 * #%L
 * Splout SQL Server
 * %%
 * Copyright (C) 2012 Datasalt Systems S.L.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.type.TypeReference;

import com.google.common.io.Files;
import com.hazelcast.core.MapLoader;
import com.hazelcast.core.MapStore;
import com.splout.db.common.JSONSerDe;
import com.splout.db.common.JSONSerDe.JSONSerDeException;

/**
 * A Hazelcast MapLoader and MapStore that saves each map key/value in a file named "key" that contains "value" as a string (JSON).
 * Saves data for {@link CoordinationStructures.#VERSIONS_BEING_SERVED}.
 */
public class TablespaceVersionStore implements MapLoader<String, Map<String, Long>>, MapStore<String, Map<String, Long>> {

	private final static Log log = LogFactory.getLog(TablespaceVersionStore.class);
	
	private File folder;
	private final static Charset UTF8 = Charset.forName("UTF-8");
	
	public TablespaceVersionStore(String baseFolder) {
		File folder = new File(baseFolder);
		if(!folder.exists()) {
			folder.mkdirs();
		}
		this.folder = folder;
	}
	
	@Override
  public void delete(String key) {
		File file = getKeyFile(key);
		if(file.exists()) {
			file.delete();
		}
  }

	@Override
  public void deleteAll(Collection<String> keys) {
		for(String key: keys) {
			delete(key);
		}
  }

	@Override
  public void store(String key, Map<String, Long> value) {
		log.info("Store: " + key + " value: " + value);
		try {
			/**
			 * Save value as UTF8 String in a File
			 */
	    Files.write(JSONSerDe.ser(value).getBytes(UTF8), getKeyFile(key));
    } catch(IOException e) {
	    throw new RuntimeException(e);
    } catch(JSONSerDeException e) {
	    throw new RuntimeException(e);
    }
  }

	@Override
  public void storeAll(Map<String, Map<String, Long>> keyValues) {
		for(Map.Entry<String, Map<String, Long>> keyValue: keyValues.entrySet()) {
			store(keyValue.getKey(), keyValue.getValue());
		}
  }

	@Override
  public Map<String, Long> load(String key) {
//		log.info("Load: " + key);
	  try {
	  	/**
	  	 * Load value from a String in a File
	  	 */
	  	Map<String, Long> map = readValues(getKeyFile(key));
//			log.info("Load to return: " + map);
	    return map;
    } catch(IOException e) {
	    throw new RuntimeException(e);
    } catch(JSONSerDeException e) {
	    throw new RuntimeException(e);
    }
  }

	@Override
  public Map<String, Map<String, Long>> loadAll(Collection<String> keys) {
//  	log.info("Load all: " + keys);
		Map<String, Map<String, Long>> toReturn = new HashMap<String, Map<String, Long>>();
		for(String key: keys) {
			Map<String, Long> result = load(key);
			toReturn.put(key, result);
		}
//  	log.info("Load all to return: " + toReturn);
		return toReturn;
  }

	@Override
  public Set<String> loadAllKeys() {
//		log.info("Load all keys:");
	  File[] files = folder.listFiles();
	  if(files == null) {
	  	return null;
	  }
	  Set<String> keys = new HashSet<String>();
	  for(File file: files) {
	  	keys.add(file.getName());
	  }
//  	log.info("Load all keys to return: " + keys);
	  return keys;
  }
	
	// --------------------------------- //
	
	protected File getKeyFile(String key) {
		return new File(folder, key);
	}
	
	public final static TypeReference<Map<String, Long>> MAP_STRING_LONG_TYPE_REF = new TypeReference<Map<String, Long>>() {
		
	};
	
	protected Map<String, Long> readValues(File file) throws JSONSerDeException, IOException {
		if(file.exists()) {
			return JSONSerDe.deSer(Files.toString(file, UTF8), MAP_STRING_LONG_TYPE_REF);
		} else {
			return null;
		}
	}
}