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

import java.util.ArrayList;

import com.splout.db.common.BaseBean;

/**
 * JSON bean that is returned by the QNode as response to a query. It contains useful information such as the time it took, the shard it hit, etc.
 */
@SuppressWarnings("rawtypes")
public class QueryStatus extends BaseBean {

  protected ArrayList result;
  protected Integer shard;
  protected Long millis;
  protected String error;
	protected Integer cursorId;
	
	public String getError() {
  	return error;
  }
	public void setError(String error) {
  	this.error = error;
  }
	public ArrayList getResult() {
  	return result;
  }
	public void setResult(ArrayList result) {
  	this.result = result;
  }
	public Integer getShard() {
  	return shard;
  }
	public void setShard(Integer shard) {
  	this.shard = shard;
  }
	public Long getMillis() {
  	return millis;
  }
	public void setMillis(Long millis) {
  	this.millis = millis;
  }
	public Integer getCursorId() {
    return cursorId;
  }
	public void setCursorId(Integer cursorId) {
    this.cursorId = cursorId;
  }
}
