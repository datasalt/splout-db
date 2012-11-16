package com.splout.db.dnode.beans;

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

import java.util.List;

/**
 * JSON bean that is returned when requesting for the status of the DNode.
 */
public class DNodeSystemStatus {
	
	private String systemStatus;
	private long lastExceptionTime;
	private boolean deployInProgress;
	private long upSince;
	private int nQueries;
	private int failedQueries;
	private double slowQueries;
	private double average;
	private long occupiedSpaceInDisk;
	private long freeSpaceInDisk;
	private List<String> files;
	
	public List<String> getFiles() {
  	return files;
  }
	public void setFiles(List<String> files) {
  	this.files = files;
  }
	public long getFreeSpaceInDisk() {
  	return freeSpaceInDisk;
  }
	public void setFreeSpaceInDisk(long freeSpaceInDisk) {
  	this.freeSpaceInDisk = freeSpaceInDisk;
  }
	public long getUpSince() {
  	return upSince;
  }
	public void setUpSince(long upSince) {
  	this.upSince = upSince;
  }
	public int getnQueries() {
  	return nQueries;
  }
	public void setnQueries(int nQueries) {
  	this.nQueries = nQueries;
  }
	public int getFailedQueries() {
  	return failedQueries;
  }
	public void setFailedQueries(int failedQueries) {
  	this.failedQueries = failedQueries;
  }
	public double getSlowQueries() {
  	return slowQueries;
  }
	public void setSlowQueries(double slowQueries) {
  	this.slowQueries = slowQueries;
  }
	public double getAverage() {
  	return average;
  }
	public void setAverage(double average) {
  	this.average = average;
  }
	public String getSystemStatus() {
		return systemStatus;
	}
	public void setSystemStatus(String systemStatus) {
		this.systemStatus = systemStatus;
	}
	public boolean isDeployInProgress() {
		return deployInProgress;
	}
	public void setDeployInProgress(boolean deployInProgress) {
		this.deployInProgress = deployInProgress;
	}
	public long getOccupiedSpaceInDisk() {
  	return occupiedSpaceInDisk;
  }
	public void setOccupiedSpaceInDisk(long occupiedSpaceInDisk) {
  	this.occupiedSpaceInDisk = occupiedSpaceInDisk;
  }
	public long getLastExceptionTime() {
  	return lastExceptionTime;
  }
	public void setLastExceptionTime(long lastExceptionTime) {
  	this.lastExceptionTime = lastExceptionTime;
  }
}
