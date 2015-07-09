/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.state;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.common.state.StateCheckpointer;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.streaming.api.checkpoint.CheckpointCommitter;

public class GlobalOperatorState<K, V> implements Map<K, V>,
		StateCheckpointer<Map<K, V>, Serializable>, CheckpointCommitter {

	private Map<Long, Map<K, V>> uncommittedLogs = new HashMap<Long, Map<K, V>>();
	private OperatorState<Map<K, V>> changeLog;
	private Map<K, V> cache = new HashMap<K, V>();
	private RemoteStateStore<K, V> remoteStore;

	public GlobalOperatorState(OperatorState<Map<K, V>> changeLog,
			RemoteStateStore<K, V> remoteStore) {
		this.changeLog = changeLog;
		this.remoteStore = remoteStore;
	}

	@Override
	public Serializable snapshotState(Map<K, V> changeLog, long checkpointId,
			long checkpointTimestamp) {
		uncommittedLogs.put(checkpointId, changeLog);
		try {
			this.changeLog.update(new HashMap<K, V>());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return (Serializable) changeLog;
	}

	@Override
	public void commitCheckpoint(long checkpointId, String stateName,
			StateHandle<Serializable> checkPointedState) throws Exception {
		Map<K, V> logToCommit = uncommittedLogs.remove(checkpointId);
		if (!remoteStore.commit(logToCommit)) {
			throw new RuntimeException("Failed to commit checkpoint(" + checkpointId
					+ ") to remote store.");
		}
	}

	@Override
	public Map<K, V> restoreState(Serializable stateSnapshot) {
		return new HashMap<K, V>();
	}

	@SuppressWarnings("unchecked")
	@Override
	public V get(Object key) {
		V val = cache.get(key);
		if (val == null && (val = remoteStore.get((K) key)) != null) {
			cache.put((K) key, val);
		}
		return val;
	}

	@Override
	public V put(K k, V v) {
		try {
			changeLog.value().put(k, v);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return cache.put(k, v);
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> entries) {
		for (Entry<? extends K, ? extends V> entry : entries.entrySet()) {
			put(entry.getKey(), entry.getValue());
		}
	}

	@Override
	public boolean containsKey(Object key) {
		return get(key) != null;
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean containsValue(Object arg0) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isEmpty() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<K> keySet() {
		throw new UnsupportedOperationException();
	}

	@Override
	public V remove(Object arg0) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int size() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Collection<V> values() {
		throw new UnsupportedOperationException();
	}

	@Override
	public String toString() {
		try {
			return "Current log: " + changeLog.value().toString() + "\nUncommitted-logs: "
					+ uncommittedLogs.toString();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
