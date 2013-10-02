package com.splout.db.qnode.beans;

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
