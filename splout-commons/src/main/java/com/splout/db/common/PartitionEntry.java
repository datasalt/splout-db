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

import java.io.Serializable;

/**
 * An entry in a {@link PartitionMap}. It consists of a min value and a max value, as well as a shard id.
 * [min, max). Min excluded, max included.
 *
 * @see PartitionMap
 */
@SuppressWarnings("serial")
public class PartitionEntry extends BaseBean implements Serializable, Comparable<PartitionEntry> {

  public String toString() {
    return "min:" + min + ",max:" + max + ",shard:" + shard;
  }

  String min;
  String max;
  Integer shard;

  // ----------------- //
  public String getMin() {
    return min;
  }

  public void setMin(String min) {
    this.min = min;
  }

  public String getMax() {
    return max;
  }

  public void setMax(String max) {
    this.max = max;
  }

  public Integer getShard() {
    return shard;
  }

  public void setShard(Integer shard) {
    this.shard = shard;
  }

  @Override
  public boolean equals(Object obj) {
    PartitionEntry pEntry = (PartitionEntry) obj;
    return shard.equals(pEntry.shard);
  }

  @Override
  public int hashCode() {
    return shard.hashCode();
  }

  @Override
  public int compareTo(PartitionEntry pEntry) {
    return shard.compareTo(pEntry.shard);
  }
}