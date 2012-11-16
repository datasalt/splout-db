package com.splout.db.qnode.beans;

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

import com.splout.db.common.BaseBean;

/**
 * If a deployment is in progress, this bean is serialized as JSON in a file in ZooKeeper so that any QNode can know that there is a deploy in progress and at what time it started, etc.
 * This bean is also returned for the deploy() method in QNode.
 */
public class DeployInfo extends BaseBean {

	private String error;
	private String startedAt;
	private Long version;

	public DeployInfo() {
		
	}
	
	public DeployInfo(String error) {
		this.error = error;
	}
	
	public String getStartedAt() {
		return startedAt;
	}

	public void setStartedAt(String startedAt) {
		this.startedAt = startedAt;
	}

	public Long getVersion() {
		return version;
	}

	public void setVersion(Long version) {
		this.version = version;
	}
	
	public String getError() {
  	return error;
  }

	public void setError(String error) {
  	this.error = error;
  }
}
