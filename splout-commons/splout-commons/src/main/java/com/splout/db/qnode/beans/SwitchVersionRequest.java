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
 * Like {@link DeployRequest}, but used to request a rollback instead of a deploy.
 */
public class SwitchVersionRequest extends BaseBean {

	String tablespace;
	long version;
	
	public SwitchVersionRequest() {	}
	
	public SwitchVersionRequest(String tablespace, long version) {
	  super();
	  this.tablespace = tablespace;
	  this.version = version;
  }
	
	// ----------------- //
	public String getTablespace() {
  	return tablespace;
  }
	public void setTablespace(String table_partition) {
  	this.tablespace = table_partition;
  }
	public long getVersion() {
  	return version;
  }
	public void setVersion(long version) {
  	this.version = version;
  }
}