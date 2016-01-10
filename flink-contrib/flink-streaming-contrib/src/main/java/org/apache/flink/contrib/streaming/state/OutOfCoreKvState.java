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

package org.apache.flink.contrib.streaming.state;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.flink.runtime.state.StateBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

public abstract class OutOfCoreKvState<K, V, S extends StateBackend<S>> implements KvState<K, V, S> {

	private static final Logger LOG = LoggerFactory.getLogger(OutOfCoreKvState.class);

	protected final KvStateConfig conf;

	protected final TypeSerializer<K> keySerializer;
	protected final TypeSerializer<V> valueSerializer;
	protected final V defaultValue;

	protected K currentKey;

	protected final StateCache cache;

	protected long lastCheckpointId;
	protected long lastCheckpointTs;
	protected long currentTs;

	public OutOfCoreKvState(
			KvStateConfig kvStateConf,
			TypeSerializer<K> keySerializer,
			TypeSerializer<V> valueSerializer,
			V defaultValue,
			long lastCheckpointId,
			long lastCheckpointTs,
			long currentTs) {

		this.conf = kvStateConf;
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;

		this.defaultValue = defaultValue;

		this.cache = new StateCache(conf.getKvCacheSize(), conf.getNumElementsToEvict());

		this.lastCheckpointId = lastCheckpointId;
		this.lastCheckpointTs = lastCheckpointTs;
		this.currentTs = currentTs;
	}

	public StateCache getCache() {
		return cache;
	}

	@Override
	public void setCurrentKey(K key) {
		this.currentKey = key;
	}

	@Override
	public void update(V value) throws IOException {
		try {
			cache.put(currentKey, Optional.fromNullable(value));
		} catch (RuntimeException e) {
			// We need to catch the RuntimeExceptions thrown in the StateCache
			// methods here
			throw new IOException(e);
		}
	}

	@Override
	public V value() throws IOException {
		try {
			// We get the value from the cache (which will automatically load it
			// from the database if necessary). If null, we return a copy of the
			// default value
			V val = cache.get(currentKey).orNull();
			return val != null ? val : copyDefault();
		} catch (RuntimeException e) {
			// We need to catch the RuntimeExceptions thrown in the StateCache
			// methods here
			throw new IOException(e);
		}
	}

	@Override
	public KvStateSnapshot<K, V, S> snapshot(long checkpointId, long timestamp)
			throws Exception {

		// Validate timing assumptions
		if (timestamp <= currentTs) {
			throw new RuntimeException("Checkpoint timestamp is smaller than previous ts + 1, "
					+ "this should not happen.");
		}

		// We (incrementally) snapshot the modified states, then clear the
		// containing map
		if (!cache.modified.isEmpty()) {
			snapshotModified(cache.modified.entrySet(), checkpointId, timestamp);
			cache.modified.clear();
		}

		// Create a snapshot that will be used to restore this state
		KvStateSnapshot<K, V, S> snapshot = createSnapshot(checkpointId, timestamp);

		lastCheckpointTs = timestamp;
		currentTs = timestamp + 1;
		lastCheckpointId = checkpointId;

		return snapshot;
	}

	/**
	 * Create a {@link KvStateSnapshot} for the current id and timestamp. It is
	 * not assumed that a checkpoint will always successfully complete (it might
	 * fail at other tasks as well). Therefore the snapshot should contain
	 * enough information so that it can clean up the partially failed records
	 * to maintain the exactly-once semantics.
	 * <p>
	 * For instance if the snapshot is taken based on the timestamp, we can use
	 * the checkpoint timestamp and recovery timestamp to delete records between
	 * those two.
	 * 
	 * @param checkpointId
	 *            The current checkpoint id. This is not assumed to be always
	 *            increasing.
	 * @param timestamp
	 *            The current checkpoint timestamp. This is assumed to be
	 *            increasing.
	 * @return The current {@link KvStateSnapshot}
	 * @throws Exception
	 */
	public abstract KvStateSnapshot<K, V, S> createSnapshot(long checkpointId, long timestamp) throws Exception;

	/**
	 * Snapshot (save) the states that were modified since the last checkpoint
	 * to the out-of-core storage layer. (For instance write to database or
	 * disk)
	 * 
	 * @param modifiedKVs
	 *            Collection of Key-Optional<State> entries to be checkpointed.
	 * @param checkpointId
	 *            The current checkpoint id. This is not assumed to be always
	 *            increasing.
	 * @param timestamp
	 *            The current checkpoint timestamp. This is assumed to be
	 *            increasing.
	 */
	public abstract void snapshotModified(Collection<Entry<K, Optional<V>>> modifiedKVs, long checkpointId,
			long timestamp) throws IOException;

	/**
	 * Save the given collection of state entries to the out-of-core storage so
	 * that it can retrieved later. This method is called when the state cache
	 * is full and wants to evict elements.
	 * <p>
	 * Records written by this method will not be part of the previous snapshot
	 * but should be part of the next one.
	 * 
	 * @param KVsToEvict
	 *            Collection of Key-Optional<State> entries to be evicted.
	 * @param lastCheckpointId
	 *            The checkpoint id of the last checkpoint.
	 * @param lastCheckpointTs
	 *            The timestamp of the last checkpoint.
	 * @param currentTs
	 *            Current timestamp (greater or equal to the last checkpoint
	 *            timestamp)
	 * @throws IOException
	 */
	public void evictModified(Collection<Entry<K, Optional<V>>> KVsToEvict, long lastCheckpointId,
			long lastCheckpointTs, long currentTs) throws IOException {
		snapshotModified(KVsToEvict, lastCheckpointId + 1, currentTs);
	}

	/**
	 * Lookup latest entry for a specific key from the out-of-core storage.
	 * 
	 * @param key
	 *            Key to lookup.
	 * @return Returns {@link Optional#of(..)} if exists or
	 *         {@link Optional#absent()} if missing.
	 */
	public abstract Optional<V> lookupLatest(K key);

	/**
	 * Return a copy the default value or null if the default was null.
	 * 
	 */
	private V copyDefault() {
		return defaultValue != null ? valueSerializer.copy(defaultValue) : null;
	}

	/**
	 * LRU cache implementation for storing the key-value states. When the cache
	 * is full elements are not evicted one by one but are evicted in a batch
	 * defined in the {@link KvStateConfig}.
	 * <p>
	 * Keys not found in the cached will be retrieved from the underlying
	 * out-of-core storage
	 */
	public final class StateCache extends LinkedHashMap<K, Optional<V>> {
		private static final long serialVersionUID = 1L;

		private final int cacheSize;
		private final int evictionSize;

		// We keep track the state modified since the last checkpoint
		protected final Map<K, Optional<V>> modified = new HashMap<>();

		public StateCache(int cacheSize, int evictionSize) {
			super(cacheSize, 0.75f, true);
			this.cacheSize = cacheSize;
			this.evictionSize = evictionSize;
		}

		@Override
		public Optional<V> put(K key, Optional<V> value) {
			// Put kv pair in the cache and evict elements if the cache is full
			Optional<V> old = super.put(key, value);
			modified.put(key, value);
			evictIfFull();
			return old;
		}

		@SuppressWarnings("unchecked")
		@Override
		public Optional<V> get(Object key) {
			// First we check whether the value is cached
			Optional<V> value = super.get(key);
			if (value == null) {
				value = lookupLatest((K) key);
				put((K) key, value);
			}
			return value;
		}

		@Override
		protected boolean removeEldestEntry(Entry<K, Optional<V>> eldest) {
			// We need to remove elements manually if the cache becomes full, so
			// we always return false here.
			return false;
		}

		private void evictIfFull() {
			if (size() > cacheSize) {

				int numEvicted = 0;
				Iterator<Entry<K, Optional<V>>> entryIterator = entrySet().iterator();
				List<Entry<K, Optional<V>>> toEvict = new ArrayList<>();

				while (numEvicted++ < evictionSize && entryIterator.hasNext()) {

					Entry<K, Optional<V>> next = entryIterator.next();

					// We only need to write to the database if modified
					if (modified.remove(next.getKey()) != null) {
						toEvict.add(next);
					}

					entryIterator.remove();
				}

				try {
					evictModified(toEvict, lastCheckpointId, lastCheckpointTs, currentTs);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}

				currentTs++;

			}
		}

		@Override
		public void putAll(Map<? extends K, ? extends Optional<V>> m) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void clear() {
			super.clear();
			modified.clear();
		}

		@Override
		public String toString() {
			return "Cache: " + super.toString() + "\nModified: " + modified;
		}
	}

}
