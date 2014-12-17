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
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

/**
 * Just a statusMessage message and nothing more, returned by rollback() method.
 */
@JsonIgnoreProperties(ignoreUnknown = true) // Backwards compatibility in JSON (new fields don't make things break)
public class StatusMessage extends BaseBean {

  public enum Status {OK, ERROR};
  private String statusMessage;
  private Status status;

  public StatusMessage() {
  }

  public StatusMessage(Status status, String statusMessage) {
    this.status = status;
    this.statusMessage = statusMessage;
  }

  public String getStatusMessage() {
    return statusMessage;
  }

  public void setStatusMessage(String statusMessage) {
    this.statusMessage = statusMessage;
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(Status status) {
    this.status = status;
  }
}
