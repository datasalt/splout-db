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
import java.util.Map;

import com.splout.db.common.BaseBean;

/**
 * This bean is returned by the QNode on demand and indicates all the history of deployments: which ones failed,
 * succeeded, how long did they last, etc.
 */
public class DeploymentsStatus extends BaseBean {

	// A basic map with the deploy history: Which deploys failed, which not... 
	private Map<Long, DeployStatus> deployHistory;
	// For each deployment, a list of messages associated with it
	private Map<Long, List<String>> deployLogs;
	
	public Map<Long, DeployStatus> getDeployHistory() {
  	return deployHistory;
  }
	public void setDeployHistory(Map<Long, DeployStatus> deployHistory) {
  	this.deployHistory = deployHistory;
  }
	public Map<Long, List<String>> getDeployLogs() {
  	return deployLogs;
  }
	public void setDeployLogs(Map<Long, List<String>> deployLogs) {
  	this.deployLogs = deployLogs;
  }
}
