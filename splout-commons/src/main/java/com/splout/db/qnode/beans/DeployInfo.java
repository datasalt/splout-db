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

import java.io.Serializable;
import java.util.List;

/**
 * This bean is returned in the deploy() method in QNode. It allows the user to track a deploy, as the version number is
 * returned.
 */
@SuppressWarnings("serial")
public class DeployInfo extends BaseBean implements Serializable {

  private String error;
  private String startedAt;
  private Long version;
  private List<String> dataURIs;
  private List<String> tablespacesDeployed;

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

  public List<String> getDataURIs() {
    return dataURIs;
  }

  public void setDataURIs(List<String> dataURIs) {
    this.dataURIs = dataURIs;
  }

  public List<String> getTablespacesDeployed() {
    return tablespacesDeployed;
  }

  public void setTablespacesDeployed(List<String> tablespacesDeployed) {
    this.tablespacesDeployed = tablespacesDeployed;
  }
}
