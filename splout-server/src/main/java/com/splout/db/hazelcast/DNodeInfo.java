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
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.splout.db.common.SploutConfiguration;
import com.splout.db.common.ThriftReader;
import com.splout.db.dnode.DNodeHandler;
import com.splout.db.dnode.DNodeProperties;
import com.splout.db.thrift.PartitionMetadata;

/**
 * This class shows all the relevant info of a DNode. This info is serialized and shared with the whole cluster so that
 * QNodes can have a global view of the Tablespaces that can be served, for instance. In this class each DNode shows its
 * current Address and the Tablespace (String)-Version (Long)-Partitions (Integer) that can be served by it. In this way
 * if a cluster is shutdown and started again, the global Tablespace info will be reconstructed by adding up each
 * DNode's info.
 */
@SuppressWarnings("serial")
public class DNodeInfo implements Serializable {

	private final static Log log = LogFactory.getLog(DNodeInfo.class);
	
	public String toString() {
		return address + ", serving info -> " + servingInfo;
	}
	
	private String address;
	private Map<String, Map<Long, Map<Integer, PartitionMetadata>>> servingInfo;

	public DNodeInfo() {
	}
	
	public DNodeInfo(String address, Map<String, Map<Long, Map<Integer, PartitionMetadata>>> servingInfo) {
		this.address = address;
		this.servingInfo = servingInfo;
	}

	/**
	 * Construct this DNode's info from the configuration data.
	 */
	public DNodeInfo(SploutConfiguration config) {
		this.servingInfo = new HashMap<String, Map<Long, Map<Integer, PartitionMetadata>>>();
		this.address = config.getString(DNodeProperties.HOST) + ":" + config.getInt(DNodeProperties.PORT);
		File dataFolder = new File(config.getString(DNodeProperties.DATA_FOLDER));
		// inspect the file system
		File[] tablespaces = dataFolder.listFiles();
		if(tablespaces == null) {
			// fresh DNode with no deployed tablespaces in local disk
			return;
		}
		try {
			for(File tablespace : tablespaces) {
				String tablespaceName = tablespace.getName();
				File[] versions = tablespace.listFiles();
				if(versions == null) {
					// tablespace with no versions - this is kind of weird, we can fail or log a warning...
					log.warn("Useless tablespace folder with no versions: " + tablespace);
					continue;
				}
				for(File version : versions) {
					Long versionName = new Long(version.getName());
					File[] partitions = version.listFiles();
					if(partitions == null) {
						// version with no partitions - this is kind of weird, we can fail or log a warning...
						log.warn("Useless version folder with no partitions: " + version);
						continue;
					}
					for(File partition : partitions) {
						if(!partition.isDirectory()) {
							if(!partition.getName().endsWith("meta")) {
								log.warn("A partition file that is not a folder nor a .meta file: " + partition);
							}
							continue;
						}
						if(partition.list() == null || partition.list().length < 1) {
							log.warn("An empty partition folder: " + partition);
							continue;
						}
						Integer partitionName = new Integer(partition.getName());
						File metadataFile = DNodeHandler.getLocalMetadataFile(config, tablespaceName, partitionName, versionName);
						ThriftReader reader = new ThriftReader(metadataFile);
						PartitionMetadata metadata = (PartitionMetadata) reader.read(new PartitionMetadata());
						reader.close();
						if(servingInfo.get(tablespaceName) == null) {
							servingInfo.put(tablespaceName, new HashMap<Long, Map<Integer,PartitionMetadata>>());
						}
						if(servingInfo.get(tablespaceName).get(versionName) == null) {
							servingInfo.get(tablespaceName).put(versionName, new HashMap<Integer, PartitionMetadata>());
						}
						servingInfo.get(tablespaceName).get(versionName).put(partitionName, metadata);
					}
				}
			}
		} catch(IOException e) {
			log.error("Corrupted metadata file", e);
			throw new RuntimeException("Corrupted metadata file", e);
		}
	}

	// -------------- Getters & setters -------------- //
	public String getAddress() {
		return address;
	}
	public void setAddress(String address) {
		this.address = address;
	}
	public Map<String, Map<Long, Map<Integer, PartitionMetadata>>> getServingInfo() {
		return servingInfo;
	}
	public void setServingInfo(Map<String, Map<Long, Map<Integer, PartitionMetadata>>> servingInfo) {
		this.servingInfo = servingInfo;
	}
}
