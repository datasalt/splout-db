package com.splout.db.engine;

/*
 * #%L
 * Splout Redis
 * %%
 * Copyright (C) 2012 - 2014 Datasalt Systems S.L.
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
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.splout.db.common.JSONSerDe;
import com.splout.db.common.JSONSerDe.JSONSerDeException;
import com.splout.db.hadoop.TableSpec;
import com.splout.db.hadoop.engine.SploutSQLOutputFormat;

@SuppressWarnings("serial")
public class RedisOutputFormat extends SploutSQLOutputFormat implements Serializable {

	private String keyField = null;
	
	public RedisOutputFormat(Integer batchSize, TableSpec... dbSpec) throws SploutSQLOutputFormatException {
		super(batchSize, dbSpec);
		for(TableSpec spec: dbSpec) {
			if(spec.getPartitionFields() != null) {
				if(keyField != null) {
					throw new SploutSQLOutputFormatException("Redis output format only works with one partitioned table.");					
				}
				this.keyField = spec.getPartitionFields()[0].getName();
				if(spec.getPartitionFields().length > 1) {
					throw new SploutSQLOutputFormatException("Redis output format only works with one partitioning field.");										
				}
			}
		}
	}

	private Map<Integer, RDBOutputStream> redisFiles = new HashMap<Integer, RDBOutputStream>();

	// This method is called one time per each partition
	public void initPartition(int partition, Path local) throws IOException {
		RDBOutputStream rdb = new RDBOutputStream(FileSystem.getLocal(getConf()).create(local));
		redisFiles.put(partition, rdb);

		rdb.writeHeader();
		rdb.writeDatabaseSelector(0);
	}

	@Override
	public void close() throws IOException, InterruptedException {
		for(Map.Entry<Integer, RDBOutputStream> entry : redisFiles.entrySet()) {
			entry.getValue().writeFooter();

			entry.getValue().close();
		}
	}

	@Override
	public void write(ITuple tuple) throws IOException, InterruptedException {
		int partition = (Integer) tuple.get(SploutSQLOutputFormat.PARTITION_TUPLE_FIELD);

		RDBOutputStream oS = redisFiles.get(partition);

		String key = tuple.get(keyField) + "";
		try {
			Map<String, Object> tplMap = new HashMap<String, Object>();
			for(Field field : tuple.getSchema().getFields()) {
				if(field.getName().equals(SploutSQLOutputFormat.PARTITION_TUPLE_FIELD)) {
					continue;
				}
				if(!field.getType().equals(Type.STRING)) {
					tplMap.put(field.getName(), tuple.get(field.getName()));
				} else {
					tplMap.put(field.getName(), tuple.getString(field.getName()));
				}
			}
			oS.writeString(RDBString.create(key), RDBString.create(JSONSerDe.ser(tplMap)));
		} catch(JSONSerDeException e) {
			throw new IOException(e);
		}
	}

	@Override
  public String getCreateTable(TableSpec tableSpec) throws SploutSQLOutputFormatException {
		// doesn't apply
		return null;
	}
}
