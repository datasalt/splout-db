package com.splout.db.qnode.beans;

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

import java.util.List;

public class DeploymentStatus {

	private long deploymentId;
	private List<String> tablespacesDeployed;
	private String date;
	private List<String> dataURIs;
	private List<String> logMessages;

	public long getDeploymentId() {
  	return deploymentId;
  }
	public void setDeploymentId(long deploymentId) {
  	this.deploymentId = deploymentId;
  }
	public List<String> getTablespacesDeployed() {
  	return tablespacesDeployed;
  }
	public void setTablespacesDeployed(List<String> tablespacesDeployed) {
  	this.tablespacesDeployed = tablespacesDeployed;
  }
	public String getDate() {
  	return date;
  }
	public void setDate(String date) {
  	this.date = date;
  }
	public List<String> getDataURIs() {
  	return dataURIs;
  }
	public void setDataURIs(List<String> dataURIs) {
  	this.dataURIs = dataURIs;
  }
	public List<String> getLogMessages() {
  	return logMessages;
  }
	public void setLogMessages(List<String> logMessages) {
  	this.logMessages = logMessages;
  }
}
