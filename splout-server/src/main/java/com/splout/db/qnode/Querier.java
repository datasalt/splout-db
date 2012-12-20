package com.splout.db.qnode;

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import com.splout.db.common.JSONSerDe;
import com.splout.db.common.JSONSerDe.JSONSerDeException;
import com.splout.db.common.PartitionMap;
import com.splout.db.common.ReplicationEntry;
import com.splout.db.common.ReplicationMap;
import com.splout.db.common.Tablespace;
import com.splout.db.hazelcast.TablespaceVersion;
import com.splout.db.qnode.beans.ErrorQueryStatus;
import com.splout.db.qnode.beans.QueryStatus;
import com.splout.db.thrift.DNodeException;
import com.splout.db.thrift.DNodeService;

/**
 * The Querier is a specialized module ({@link com.splout.db.qnode.QNodeHandlerModule}) of the
 * {@link com.splout.db.qnode.QNode} that performs the distributed query mechanism.
 */
@SuppressWarnings({ "rawtypes" })
public class Querier extends QNodeHandlerModule {

	public final static String PARTITION_RANDOM = "random";
	
	private final static Log log = LogFactory.getLog(Querier.class);
	
	@SuppressWarnings("serial")
  public static final class QuerierException extends Exception {
		
		public QuerierException(String msg) {
			super(msg);
    }
	}

	public Querier(QNodeHandlerContext context) {
		super(context);
	}

	/**
	 * Proxy method for QNodeHandler's query() method. Returns a {@link QueryStatus} with the status of the query.
	 * 
	 * @throws JSONSerDeException
	 * @throws QuerierException 
	 */
	public QueryStatus query(String tablespaceName, String key, String sql, String partition) throws JSONSerDeException, QuerierException {
		Long version = context.getCurrentVersionsMap().get(tablespaceName);
		if(version == null) {
			return new ErrorQueryStatus("Unknown tablespace or no version ready to be served! (" + tablespaceName + ")");
		}
		Tablespace tablespace = context.getTablespaceVersionsMap().get(
		    new TablespaceVersion(tablespaceName, version));
		if(tablespace == null) {
			return new ErrorQueryStatus("Unknown tablespace version:(" + version +") tablespace:(" + tablespaceName + ")");
		}
		PartitionMap partitionMap = tablespace.getPartitionMap();
		
		// find the partition
		int partitionId;
		// use a key to find the appropriated partition
		if(key != null) {
			partitionId = partitionMap.findPartition(key);
			if(partitionId == PartitionMap.NO_PARTITION) {
				return new ErrorQueryStatus("Key out of partition ranges: " + key + " for tablespace "
				    + tablespaceName);
			}
		} else { // use provided partition
			// partition shouldn't be null here -> we check it before at QNodeHandler
			if(partition.toLowerCase().equals(PARTITION_RANDOM)) {
				partitionId = (int)(Math.random() * partitionMap.getPartitionEntries().size());
			} else {
				try {
					partitionId = Integer.parseInt(partition);
				} catch(Exception e) {
					throw new QuerierException("partition must be either a valid partition number or '" + PARTITION_RANDOM + "' string.");
				}
			}
		}
		return query(tablespaceName, sql, partitionId);
	}

	private ThreadLocal<Map<Integer, Integer>> partitionRoundRobin = new ThreadLocal<Map<Integer, Integer>>() {

		protected Map<Integer, Integer> initialValue() {
			return new HashMap<Integer, Integer>();
		};
	};

	public Map<Integer, Integer> getPartitionRoundRobin() {
		return partitionRoundRobin.get();
	}

	/**
	 * API method for querying a tablespace when you already know the partition Id. Can be used for multi-querying.
	 */
	public QueryStatus query(String tablespaceName, String sql, int partitionId) throws JSONSerDeException {
		Long version = context.getCurrentVersionsMap().get(tablespaceName);
		if(version == null) {
			return new ErrorQueryStatus("Unknown tablespace! (" + tablespaceName + ")");
		}
		Tablespace tablespace = context.getTablespaceVersionsMap().get(
		    new TablespaceVersion(tablespaceName, version));
		if(tablespace == null) {
			return new ErrorQueryStatus("Unknown tablespace! (" + tablespaceName + ")");
		}
		ReplicationMap replicationMap = tablespace.getReplicationMap();
		ReplicationEntry repEntry = null;

		for(ReplicationEntry rEntry : replicationMap.getReplicationEntries()) {
			if(rEntry.getShard() == partitionId) {
				repEntry = rEntry;
			}
		}
		
		if(repEntry == null) {
			return new ErrorQueryStatus("Incomplete Tablespace information for tablespace (" + tablespaceName
			    + ") Maybe let the Splout warmup a little bit and try later?");
		}
		if(repEntry.getNodes().size() == 0) { // No one alive for serving the query!
			return new ErrorQueryStatus("No alive DNodes for " + tablespace);
		}

		String electedNode;
		int tried = 0;
		for(;;) { // Fail-over loop
			electedNode = null;
			Integer lastNode = partitionRoundRobin.get().get(partitionId);
			if(lastNode == null) {
				lastNode = -1;
			}
			lastNode++;
			tried++;
			int index = lastNode % repEntry.getNodes().size();
			electedNode = repEntry.getNodes().get(index);
			partitionRoundRobin.get().put(partitionId, index);

			// Perform query
			QueryStatus qStatus = new QueryStatus();
			long start = System.currentTimeMillis();

			DNodeService.Client client = null;
			boolean renew = false;
			
			try {
				client = context.getDNodeClientFromPool(electedNode);

				String r;
				try {
					r = client.sqlQuery(tablespaceName, version, partitionId, sql);
				} catch(TTransportException e) {
					renew = true;
					throw e;
				}
				
				qStatus.setResult(JSONSerDe.deSer(r, ArrayList.class));
				long end = System.currentTimeMillis();
				// Report the time of the query
				qStatus.setMillis((end - start));
				// ... and the shard hit.
				qStatus.setShard(partitionId);
				return qStatus;
			} catch(DNodeException e) {
				log.error("Exception in Querier", e);
				if(tried == repEntry.getNodes().size()) {
					return new ErrorQueryStatus("DNode exception (" + e.getMsg() + ") from " + electedNode);
				}
			} catch(TException e) {
				log.error("Exception in Querier", e);
				if(tried == repEntry.getNodes().size()) {
					return new ErrorQueryStatus("Error connecting to client " + electedNode);
				}
			} finally {
				if(client != null) {
					context.returnDNodeClientToPool(electedNode, client, renew);
				}
			}
		}
	}

	/**
	 * Helper method for casting a String to the appropriate Tablespace key type.
	 */
	public Comparable<?> castKey(String key, String tablespace, Class<? extends Comparable> clazz)
	    throws Exception {
		Comparable<?> keyObj;
		if(clazz.equals(Integer.class)) {
			keyObj = Integer.parseInt(key);
		} else if(clazz.equals(Long.class)) {
			keyObj = Long.parseLong(key);
		} else if(clazz.equals(Float.class)) {
			keyObj = Float.parseFloat(key);
		} else if(clazz.equals(Double.class)) {
			keyObj = Double.parseDouble(key);
		} else if(clazz.equals(String.class)) {
			keyObj = key + "";
		} else {
			// ?
			throw new RuntimeException("Can't handle tablespace (" + tablespace + ") with key of type "
			    + clazz + ". This is very likely a software bug");
		}
		return keyObj;
	}
}