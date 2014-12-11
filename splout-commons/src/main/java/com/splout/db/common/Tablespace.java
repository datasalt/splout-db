package com.splout.db.common;

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

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * A tablespace is made up by:
 * <ul>
 * <li>A {@link PartitionMap}</li>
 * <li>A {@link ReplicationMap}</li>
 * <li>A version string</li>
 * <li>A creation date</li>
 * </ul>
 * The Tablespace entity is necessary for safely storing each tablespace's information in memory and atomically updating
 * it.
 */
@SuppressWarnings({"serial"})
@JsonIgnoreProperties(ignoreUnknown = true) // Backwards compatibility in JSON (new fields don't make things break)
public class Tablespace implements Serializable {

  @Override
  public String toString() {
    return "Tablespace [partitionMap=" + partitionMap + ", replicationMap=" + replicationMap + ", version=" + version
        + ", creationDate=" + creationDate + "]";
  }

  private PartitionMap partitionMap;
  private ReplicationMap replicationMap;
  private long version;
  private long creationDate;

  public Tablespace() {
  }

  public Tablespace(PartitionMap partitionMap, ReplicationMap replicationMap, long version, long creationDate) {
    this.partitionMap = partitionMap;
    this.replicationMap = replicationMap;
    this.version = version;
    this.creationDate = creationDate;
  }

  // ---- Getters & Setters ---- //

  public PartitionMap getPartitionMap() {
    return partitionMap;
  }

  public ReplicationMap getReplicationMap() {
    return replicationMap;
  }

  public long getVersion() {
    return version;
  }

  public long getCreationDate() {
    return creationDate;
  }

  public void setPartitionMap(PartitionMap partitionMap) {
    this.partitionMap = partitionMap;
  }

  public void setReplicationMap(ReplicationMap replicationMap) {
    this.replicationMap = replicationMap;
  }

  public void setVersion(long version) {
    this.version = version;
  }

  public void setCreationDate(long creationDate) {
    this.creationDate = creationDate;
  }
}
