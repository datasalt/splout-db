package com.splout.db.hadoop.engine;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.splout.db.common.JSONSerDe;
import com.splout.db.common.JSONSerDe.JSONSerDeException;
import com.splout.db.engine.redis.RDBOutputStream;
import com.splout.db.engine.redis.RDBString;
import com.splout.db.hadoop.TableSpec;

@SuppressWarnings("serial")
public class RedisOutputFormat extends SploutSQLOutputFormat implements Serializable {

	String keyField = null;

	public RedisOutputFormat(int batchSize, TableSpec... dbSpec) throws SploutSQLOutputFormatException {
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
	private Configuration conf;

	// This method is called one time per each partition
	public void initPartition(int partition, Path local) throws IOException {
		RDBOutputStream rdb = new RDBOutputStream(FileSystem.getLocal(conf).create(local));
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

	@Override
	public void init(Configuration conf) throws IOException, InterruptedException {
		this.conf = conf;
  }
}
