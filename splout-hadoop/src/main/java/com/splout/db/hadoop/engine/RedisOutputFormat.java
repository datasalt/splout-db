package com.splout.db.hadoop.engine;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.splout.db.common.HeartBeater;
import com.splout.db.common.JSONSerDe;
import com.splout.db.common.JSONSerDe.JSONSerDeException;
import com.splout.db.engine.redis.RDBOutputStream;
import com.splout.db.engine.redis.RDBString;

@SuppressWarnings("serial")
public class RedisOutputFormat extends FileOutputFormat<ITuple, NullWritable> implements Serializable {

	String keyField;

	public RedisOutputFormat(Field keyField) {
		this.keyField = keyField.getName();
	}

	@Override
	public RecordWriter<ITuple, NullWritable> getRecordWriter(TaskAttemptContext context)
	    throws IOException, InterruptedException {
		return new TupleRedisRecordWriter(context);
	}

	private static AtomicLong FILE_SEQUENCE = new AtomicLong(0);

	public class TupleRedisRecordWriter extends RecordWriter<ITuple, NullWritable> {

		// Temporary and permanent Paths for properly writing Hadoop output files
		private Map<Integer, Path> permPool = new HashMap<Integer, Path>();
		private Map<Integer, Path> tempPool = new HashMap<Integer, Path>();
		
		private Map<Integer, RDBOutputStream> redisFiles = new HashMap<Integer, RDBOutputStream>();

		private HeartBeater heartBeater;

		private FileSystem fs;
		private TaskAttemptContext context;
		
		public TupleRedisRecordWriter(TaskAttemptContext context) {
			long waitTimeHeartBeater = context.getConfiguration().getLong(HeartBeater.WAIT_TIME_CONF, 5000);
			heartBeater = new HeartBeater(context, waitTimeHeartBeater);
			heartBeater.needHeartBeat();
			this.context = context;
		}

		// This method is called one time per each partition
		private void init(int partition) throws IOException {
			// HDFS final location of the generated partition file. It will be
			// loaded to the temporary folder in the HDFS than finally will be
			// committed by the OutputCommitter to the proper location.
			FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(context);
			Path perm = new Path(committer.getWorkPath(), partition + ".db");
			fs = perm.getFileSystem(context.getConfiguration());

			// Make a task unique name that contains the actual index output name to
			// make debugging simpler
			// Note: if using JVM reuse, the sequence number will not be reset for a
			// new task using the jvm
			Path temp = context.getConfiguration().getLocalPath("mapred.local.dir", "splout_task_" + context.getTaskAttemptID()
			    + '.' + FILE_SEQUENCE.incrementAndGet());

			Path local = fs.startLocalOutput(perm, temp);

			permPool.put(partition, perm);
			tempPool.put(partition, temp);
			
			RDBOutputStream rdb = new RDBOutputStream(FileSystem.getLocal(context.getConfiguration()).create(local));
			redisFiles.put(partition, rdb);
			
			rdb.writeHeader();
			rdb.writeDatabaseSelector(0);
		}

		@Override
		public void close(TaskAttemptContext ctx) throws IOException, InterruptedException {
			try {
				if(ctx != null) {
					heartBeater.setProgress(ctx);
				}
				for(Map.Entry<Integer, RDBOutputStream> entry : redisFiles.entrySet()) {
					entry.getValue().writeFooter();
					
					entry.getValue().close();
					// Hadoop - completeLocalOutput()
					fs.completeLocalOutput(permPool.get(entry.getKey()), tempPool.get(entry.getKey()));
				}
			} finally { // in any case, destroy the HeartBeater
				heartBeater.cancelHeartBeat();
			}
		}

		@Override
		public void write(ITuple tuple, NullWritable ignore) throws IOException, InterruptedException {
			int partition = (Integer) tuple.get(SploutSQLOutputFormat.PARTITION_TUPLE_FIELD);
			
			RDBOutputStream oS = redisFiles.get(partition);
			if(oS == null) {
				init(partition);
				oS = redisFiles.get(partition);
			}
			
			String key = tuple.get(keyField) + "";
			try {
				Map<String, Object> tplMap = new HashMap<String, Object>();
				for(Field field: tuple.getSchema().getFields()) {
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
	}
}
