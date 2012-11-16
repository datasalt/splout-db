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

/**
 * Convenience bean used in tools such as {@link DeployerCMD}
 */
public class TablespaceDepSpec {
	
	private String tablespace;
	private String sourcePath;
	private int replication;
			
	public TablespaceDepSpec() {}
	public TablespaceDepSpec(String tablespace, String sourcePath, int replication) {
	  super();
	  this.tablespace = tablespace;
	  this.sourcePath = sourcePath;
	  this.replication = replication;
  }
	public String getTablespace() {
  	return tablespace;
  }
	public void setTablespace(String tablespace) {
  	this.tablespace = tablespace;
  }
	public String getSourcePath() {
  	return sourcePath;
  }
	public void setSourcePath(String sourceData) {
  	this.sourcePath = sourceData;
  }
	public int getReplication() {
  	return replication;
  }
	public void setReplication(int replication) {
  	this.replication = replication;
  }	
	
}
