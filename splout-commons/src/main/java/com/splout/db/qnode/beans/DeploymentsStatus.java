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

import com.splout.db.common.BaseBean;

import java.util.List;

/**
 * This bean is returned by the QNode on demand and indicates all the history of deployments: which ones failed,
 * succeeded, how long did they last, etc.
 */
public class DeploymentsStatus extends BaseBean {

  private List<DeploymentStatus> finishedDeployments;
  private List<DeploymentStatus> ongoingDeployments;
  private List<DeploymentStatus> failedDeployments;

  public List<DeploymentStatus> getFinishedDeployments() {
    return finishedDeployments;
  }

  public void setFinishedDeployments(List<DeploymentStatus> finishedDeployments) {
    this.finishedDeployments = finishedDeployments;
  }

  public List<DeploymentStatus> getOngoingDeployments() {
    return ongoingDeployments;
  }

  public void setOngoingDeployments(List<DeploymentStatus> ongoingDeployments) {
    this.ongoingDeployments = ongoingDeployments;
  }

  public List<DeploymentStatus> getFailedDeployments() {
    return failedDeployments;
  }

  public void setFailedDeployments(List<DeploymentStatus> failedDeployments) {
    this.failedDeployments = failedDeployments;
  }
}
