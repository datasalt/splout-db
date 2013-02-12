package com.splout.db.dnode.beans;

/*
 * #%L
 * Splout SQL commons
 * %%
 * Copyright (C) 2012 - 2013 Datasalt Systems S.L.
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


import java.io.File;

/**
 * This bean describes the state of a file transaction (tablespace/partition/version) between DNodes in progress.
 * These transactions have two files: the .meta and the .db so we need to know when both have been received to
 * transfer the partition data to the appropriated folder.
 */
public class BalanceFileReceivingProgress {

	private boolean receivedMetaFile;
	private boolean receivedBinaryFile;
	private String metaFile;
	private String binaryFile;
	private long binaryFileSize;
	private long receivedSizeSoFar;

	private String tablespace;
	private int partition;
	private long version;

	public BalanceFileReceivingProgress() {
		
	}
	
	public BalanceFileReceivingProgress(String tablespace, int partition, long version) {
		this.tablespace = tablespace;
		this.partition = partition;
		this.version = version;
	}

	// --- Modifiers --- //
	public void metaFileReceived(File metaFile) {
		this.receivedMetaFile = true;
		this.metaFile = metaFile.getAbsolutePath();
	}

	public void binaryFileReceived(File binaryFile) {
		this.receivedBinaryFile = true;
		this.binaryFile = binaryFile.getAbsolutePath();
	}
	public void progressBinaryFile(long finalSize, long sizeSoFar) {
		this.binaryFileSize = finalSize;
		this.receivedSizeSoFar = sizeSoFar;
	}
	
	// --- Getters & setters --- //
	public void setReceivedMetaFile(boolean receivedMetaFile) {
  	this.receivedMetaFile = receivedMetaFile;
  }
	public void setReceivedBinaryFile(boolean receivedBinaryFile) {
  	this.receivedBinaryFile = receivedBinaryFile;
  }
	public void setMetaFile(String metaFile) {
  	this.metaFile = metaFile;
  }
	public void setBinaryFile(String binaryFile) {
  	this.binaryFile = binaryFile;
  }
	public void setBinaryFileSize(long binaryFileSize) {
  	this.binaryFileSize = binaryFileSize;
  }
	public void setReceivedSizeSoFar(long receivedSizeSoFar) {
  	this.receivedSizeSoFar = receivedSizeSoFar;
  }
	public void setTablespace(String tablespace) {
  	this.tablespace = tablespace;
  }
	public void setPartition(int partition) {
  	this.partition = partition;
  }
	public void setVersion(long version) {
  	this.version = version;
  }
	public boolean isReceivedMetaFile() {
		return receivedMetaFile;
	}
	public boolean isReceivedBinaryFile() {
		return receivedBinaryFile;
	}
	public long getBinaryFileSize() {
		return binaryFileSize;
	}
	public long getReceivedSizeSoFar() {
		return receivedSizeSoFar;
	}
	public String getTablespace() {
		return tablespace;
	}
	public int getPartition() {
		return partition;
	}
	public long getVersion() {
		return version;
	}
	public String getMetaFile() {
		return metaFile;
	}
	public String getBinaryFile() {
		return binaryFile;
	}
}