package com.splout.db.qnode;

/*
 * #%L
 * Splout SQL Server
 * %%
 * Copyright (C) 2012 - 2013 Datasalt Systems S.L.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.hazelcast.core.EntryListener;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.IMap;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.query.Predicate;
import com.splout.db.hazelcast.DNodeInfo;

/**
 * A proxy for Hazelcast's maps for mocking calls to {@link CoordinationStructures.#getDNodes()}
 */
@SuppressWarnings("serial")
public class FixedDNodeList extends ConcurrentHashMap<String, DNodeInfo> implements IMap<String, DNodeInfo> {

	public FixedDNodeList(List<String> dNodes, List<DNodeInfo> values) {
    for(int i=0; i<dNodes.size(); i++) {
      put(dNodes.get(i), values.get(i));
    }
	}

  @Override
  public void delete(Object key) {

  }

  @Override
  public void flush() {

  }

  @Override
  public Map<String, DNodeInfo> getAll(Set<String> keys) {
    return null;
  }

  @Override
  public Future<DNodeInfo> getAsync(String key) {
    return null;
  }

  @Override
  public Future<DNodeInfo> putAsync(String key, DNodeInfo value) {
    return null;
  }

  @Override
  public Future<DNodeInfo> putAsync(String key, DNodeInfo value, long ttl, TimeUnit timeunit) {
    return null;
  }

  @Override
  public Future<DNodeInfo> removeAsync(String key) {
    return null;
  }

  @Override
  public boolean tryRemove(String key, long timeout, TimeUnit timeunit) {
    return false;
  }

  @Override
  public boolean tryPut(String key, DNodeInfo value, long timeout, TimeUnit timeunit) {
    return false;
  }

  @Override
  public DNodeInfo put(String key, DNodeInfo value, long ttl, TimeUnit timeunit) {
    return null;
  }

  @Override
  public void putTransient(String key, DNodeInfo value, long ttl, TimeUnit timeunit) {

  }

  @Override
  public DNodeInfo putIfAbsent(String key, DNodeInfo value, long ttl, TimeUnit timeunit) {
    return null;
  }

  @Override
  public void set(String key, DNodeInfo value) {

  }

  @Override
  public void set(String key, DNodeInfo value, long ttl, TimeUnit timeunit) {

  }

  @Override
  public void lock(String key) {

  }

  @Override
  public void lock(String key, long leaseTime, TimeUnit timeUnit) {

  }

  @Override
  public boolean isLocked(String key) {
    return false;
  }

  @Override
  public boolean tryLock(String key) {
    return false;
  }

  @Override
  public boolean tryLock(String key, long time, TimeUnit timeunit) throws InterruptedException {
    return false;
  }

  @Override
  public void unlock(String key) {

  }

  @Override
  public void forceUnlock(String key) {

  }

  @Override
  public String addLocalEntryListener(EntryListener<String, DNodeInfo> listener) {
    return null;
  }

  @Override
  public String addLocalEntryListener(EntryListener<String, DNodeInfo> listener, Predicate<String, DNodeInfo> predicate, boolean includeValue) {
    return null;
  }

  @Override
  public String addLocalEntryListener(EntryListener<String, DNodeInfo> listener, Predicate<String, DNodeInfo> predicate, String key, boolean includeValue) {
    return null;
  }

  @Override
  public String addInterceptor(MapInterceptor interceptor) {
    return null;
  }

  @Override
  public void removeInterceptor(String id) {

  }

  @Override
  public String addEntryListener(EntryListener<String, DNodeInfo> listener, boolean includeValue) {
    return null;
  }

  @Override
  public boolean removeEntryListener(String id) {
    return false;
  }

  @Override
  public String addEntryListener(EntryListener<String, DNodeInfo> listener, String key, boolean includeValue) {
    return null;
  }

  @Override
  public String addEntryListener(EntryListener<String, DNodeInfo> listener, Predicate<String, DNodeInfo> predicate, boolean includeValue) {
    return null;
  }

  @Override
  public String addEntryListener(EntryListener<String, DNodeInfo> listener, Predicate<String, DNodeInfo> predicate, String key, boolean includeValue) {
    return null;
  }

  @Override
  public EntryView<String, DNodeInfo> getEntryView(String key) {
    return null;
  }

  @Override
  public boolean evict(String key) {
    return false;
  }

  @Override
  public Set<String> keySet(Predicate predicate) {
    return null;
  }

  @Override
  public Set<Entry<String, DNodeInfo>> entrySet(Predicate predicate) {
    return null;
  }

  @Override
  public Collection<DNodeInfo> values(Predicate predicate) {
    return null;
  }

  @Override
  public Set<String> localKeySet() {
    return null;
  }

  @Override
  public Set<String> localKeySet(Predicate predicate) {
    return null;
  }

  @Override
  public void addIndex(String attribute, boolean ordered) {

  }

  @Override
  public LocalMapStats getLocalMapStats() {
    return null;
  }

  @Override
  public Object executeOnKey(String key, EntryProcessor entryProcessor) {
    return null;
  }

  @Override
  public Map<String, Object> executeOnKeys(Set<String> keys, EntryProcessor entryProcessor) {
    return null;
  }

  @Override
  public void submitToKey(String key, EntryProcessor entryProcessor, ExecutionCallback callback) {

  }

  @Override
  public Future submitToKey(String key, EntryProcessor entryProcessor) {
    return null;
  }

  @Override
  public Map<String, Object> executeOnEntries(EntryProcessor entryProcessor) {
    return null;
  }

  @Override
  public Map<String, Object> executeOnEntries(EntryProcessor entryProcessor, Predicate predicate) {
    return null;
  }

  @Override
  public Object getId() {
    return null;
  }

  @Override
  public String getPartitionKey() {
    return null;
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public String getServiceName() {
    return null;
  }

  @Override
  public void destroy() {

  }
}
