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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.contrib.streaming.state.logging.PerformanceLogger;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackend.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.streaming.api.checkpoint.CheckpointNotifier;
import org.apache.flink.util.InstantiationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.hash.BloomFilter;

public abstract class OutOfCoreKvState<K, V, S extends StateBackend<S>>
		implements KvState<K, V, S>, CheckpointNotifier {

	private static final Logger LOG = LoggerFactory.getLogger(OutOfCoreKvState.class);
	private final PerformanceLogger lookupLogger;

	protected final String stateId;

	protected final KvStateConfig<?> conf;

	protected final TypeSerializer<K> keySerializer;
	protected final TypeSerializer<V> valueSerializer;
	protected final V defaultValue;

	protected K currentKey;

	protected final InMemoryStateCache<K, V> cache;

	protected long lastCheckpointId;
	protected long lastCheckpointTs;
	protected long currentTs;

	protected final SortedMap<Long, Long> pendingCheckpoints = new TreeMap<>();
	protected final SortedMap<Long, Long> completedCheckpoints = new TreeMap<>();

	protected BloomFilter<byte[]> bloomFilter = null;
	protected final StateBackend<S> backend;
	protected final CompactionStrategy ct;

	public OutOfCoreKvState(
			StateBackend<S> backend,
			String stateId,
			KvStateConfig<?> kvStateConf,
			TypeSerializer<K> keySerializer,
			TypeSerializer<V> valueSerializer,
			V defaultValue,
			long lastCheckpointId,
			long lastCheckpointTs,
			long currentTs) {

		this.stateId = stateId;
		this.conf = kvStateConf;
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;

		this.lookupLogger = new PerformanceLogger(100000, stateId + " lookup", LOG);

		this.defaultValue = defaultValue;

		this.cache = new InMemoryStateCache<>(conf.getKvCacheSize(), conf.getNumElementsToEvict(), this);

		this.lastCheckpointId = lastCheckpointId;
		this.lastCheckpointTs = lastCheckpointTs;
		this.currentTs = currentTs;
		this.backend = backend;
		this.ct = conf.getCompactionStrategy();

		if (conf.hasBloomFilter()) {
			LOG.debug("Bloomfilter enabled for {}.", stateId);
			bloomFilter = BloomFilter.create(new KeyFunnel(), conf.getBloomFilterExpectedInserts(),
					conf.getBloomFilterFPP());
		}
	}

	public void restoreFilter(StreamStateHandle filterCheckpoint, ClassLoader cl) {
		if (filterCheckpoint != null && conf.hasBloomFilter()) {
			try (InputStream in = filterCheckpoint.getState(cl)) {
				this.bloomFilter = BloomFilter.readFrom(in, new KeyFunnel());
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			LOG.debug("Restored bloomfilter from checkpoint for {}.", stateId);
		}
	}

	public InMemoryStateCache<K, V> getCache() {
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
			lookupLogger.startMeasurement();
			V val = cache.get(currentKey).orNull();
			lookupLogger.stopMeasurement();
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

		LOG.debug("Starting snapshot {} for {}.", checkpointId, stateId);

		// Validate timing assumptions
		if (timestamp <= currentTs) {
			throw new RuntimeException("Checkpoint timestamp is smaller than previous ts + 1, "
					+ "this should not happen.");
		}

		preSnapshot(checkpointId, timestamp);

		// If the bloomfilter is enabled we need to checkpoint that
		StreamStateHandle filterCheckpoint = null;

		if (bloomFilter != null) {
			LOG.debug("Checkpointing bloomfilter for {}...", stateId);
			// We use the efficient serialization of the bloomfilter instead of
			// Java serialization
			CheckpointStateOutputStream cpStream = backend.createCheckpointStateOutputStream(checkpointId, timestamp);
			bloomFilter.writeTo(cpStream);
			filterCheckpoint = cpStream.closeAndGetHandle();
			LOG.debug("Bloomfilter successfully checkpointed for {}.", stateId);
		}

		// Create a snapshot that will be used to restore this state
		LOG.debug("Snapshotting modified states for {}...", stateId);
		KvStateSnapshot<K, V, S> snapshot = snapshotStates(cache.modified.entrySet(), checkpointId,
				timestamp);
		LOG.debug("Modified states succesfully checkpointed for {}...", stateId);

		KvStateSnapshot<K, V, S> wrappedSnapshot = new SnapshotWrapper<>(snapshot, filterCheckpoint,
				completedCheckpoints);

		cache.modified.clear();

		postSnapshot(checkpointId, timestamp);

		if (ct.enabled) {
			pendingCheckpoints.put(checkpointId, lastCheckpointTs + 1);
		}

		lastCheckpointTs = timestamp;
		currentTs = timestamp + 1;
		lastCheckpointId = checkpointId;

		LOG.debug("Completed snapshot {} for {}.", checkpointId, stateId);

		return wrappedSnapshot;
	}

	private static class SnapshotWrapper<K, V, S extends StateBackend<S>> implements KvStateSnapshot<K, V, S> {
		private static final long serialVersionUID = 1L;
		private final KvStateSnapshot<K, V, S> wrapped;
		private final StreamStateHandle filterCheckpoint;
		private final Map<Long, Long> completedCheckpoints;

		public SnapshotWrapper(KvStateSnapshot<K, V, S> wrapped, StreamStateHandle filterCheckpoint,
				Map<Long, Long> completedCheckpoints) {
			this.wrapped = wrapped;
			this.filterCheckpoint = filterCheckpoint;
			this.completedCheckpoints = completedCheckpoints;
		}

		@Override
		public KvState<K, V, S> restoreState(S stateBackend, TypeSerializer<K> keySerializer,
				TypeSerializer<V> valueSerializer, V defaultValue, ClassLoader classLoader, long recoveryTimestamp)
						throws Exception {
			OutOfCoreKvState<K, V, S> restored = (OutOfCoreKvState<K, V, S>) wrapped.restoreState(stateBackend,
					keySerializer, valueSerializer, defaultValue,
					classLoader, recoveryTimestamp);
			restored.completedCheckpoints.putAll(completedCheckpoints);
			if (filterCheckpoint != null) {
				restored.restoreFilter(filterCheckpoint, classLoader);
			}
			return restored;
		}

		@Override
		public void discardState() throws Exception {
			wrapped.discardState();
			if (filterCheckpoint != null) {
				filterCheckpoint.discardState();
			}
		}

		@Override
		public long getStateSize() throws Exception {
			return wrapped.getStateSize() + (filterCheckpoint != null ? filterCheckpoint.getStateSize() : 0);
		}

	}

	/**
	 * Snapshot (save) the states that were modified since the last checkpoint
	 * to the out-of-core storage layer. (For instance write to database or
	 * disk).
	 * 
	 * Returns a {@link KvStateSnapshot} for the current id and timestamp. It is
	 * not assumed that a checkpoint will always successfully complete (it might
	 * fail at other tasks as well). Therefore the snapshot should contain
	 * enough information so that it can clean up the partially failed records
	 * to maintain the exactly-once semantics.
	 * <p>
	 * For instance if the snapshot is taken based on the timestamp, we can use
	 * the checkpoint timestamp and recovery timestamp to delete records between
	 * those two.
	 * 
	 * @param modifiedKVs
	 *            Collection of Key-Optional<State> entries to be checkpointed.
	 * @param checkpointId
	 *            The current checkpoint id. This is not assumed to be always
	 *            increasing.
	 * @param timestamp
	 *            The current checkpoint timestamp. This is assumed to be
	 *            increasing.
	 * @return The current {@link KvStateSnapshot}
	 * 
	 */
	public abstract KvStateSnapshot<K, V, S> snapshotStates(Collection<Entry<K, Optional<V>>> modifiedKVs,
			long checkpointId, long timestamp) throws IOException;

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
	public abstract void evictModified(Collection<Entry<K, Optional<V>>> KVsToEvict, long lastCheckpointId,
			long lastCheckpointTs, long currentTs) throws IOException;

	/**
	 * Lookup latest entry for a specific key from the out-of-core storage.
	 * 
	 * @param key
	 *            Key to lookup.
	 * @param serializedKey
	 *            Serialized key to lookup.
	 * @return Returns {@link Optional#of(..)} if exists or
	 *         {@link Optional#absent()} if missing.
	 */
	public abstract Optional<V> lookupLatest(K key, byte[] serializedKey) throws Exception;

	/**
	 * Return a copy the default value or null if the default was null.
	 * 
	 */
	private V copyDefault() {
		return defaultValue != null ? valueSerializer.copy(defaultValue) : null;
	}

	/**
	 * Pre-snapshot hook
	 */
	public void preSnapshot(long checkpointId, long timestamp) throws Exception {

	}

	/**
	 * Post-snapshot hook
	 */
	public void postSnapshot(long checkpointId, long timestamp) throws Exception {

	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws IOException {
		if (ct.enabled) {
			if (pendingCheckpoints.containsKey(checkpointId)) {
				completedCheckpoints.put(checkpointId, pendingCheckpoints.remove(checkpointId));

				if (completedCheckpoints.size() == ct.frequency) {
					if (ct.compactFromBeginning) {
						compact(0, completedCheckpoints.get(completedCheckpoints.lastKey()));
					} else {
						compact(completedCheckpoints.get(completedCheckpoints.firstKey()),
								completedCheckpoints.get(completedCheckpoints.lastKey()));
					}
					completedCheckpoints.clear();
				}

			}
		}
	}

	public abstract void compact(long from, long to) throws IOException;

	/**
	 * Returns the number of elements currently stored in the task's cache. Note
	 * that the number of elements in the out-of-core storage is not counted
	 * here.
	 */
	@Override
	public int size() {
		return cache.size();
	}

	/**
	 * LRU cache implementation for storing the key-value states. When the cache
	 * is full elements are not evicted one by one but are evicted in a batch
	 * defined in the {@link KvStateConfig}.
	 * <p>
	 * Keys not found in the cached will be retrieved from the underlying
	 * out-of-core storage
	 */
	public static final class InMemoryStateCache<K, V> extends LinkedHashMap<K, Optional<V>> {
		private static final long serialVersionUID = 1L;

		private final OutOfCoreKvState<K, V, ?> kvState;

		private final int cacheSize;
		private final int evictionSize;

		// We keep track the state modified since the last checkpoint
		protected final Map<K, Optional<V>> modified = new HashMap<>();

		public InMemoryStateCache(int cacheSize, int evictionSize, OutOfCoreKvState<K, V, ?> kvState) {
			super(cacheSize, 0.75f, true);

			this.cacheSize = cacheSize;
			this.evictionSize = evictionSize;

			this.kvState = kvState;
		}

		@Override
		public Optional<V> put(K key, Optional<V> value) {
			// Put kv pair in the cache and evict elements if the cache is full
			Optional<V> old = super.put(key, value);
			modified.put(key, value);
			try {
				if (kvState.bloomFilter != null) {
					kvState.bloomFilter.put(InstantiationUtil.serializeToByteArray(kvState.keySerializer, key));
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			evictIfFull();
			return old;
		}

		@SuppressWarnings("unchecked")
		@Override
		public Optional<V> get(Object key) {
			// First we check whether the value is cached
			Optional<V> value = super.get(key);

			// If value is not cached we first chek the bloomfilter than we try
			// to retrieve from the external store
			if (value == null) {
				byte[] serializedKey = null;
				try {
					serializedKey = InstantiationUtil.serializeToByteArray(kvState.keySerializer, (K) key);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}

				if (kvState.bloomFilter == null || kvState.bloomFilter.mightContain(serializedKey)) {
					try {
						value = kvState.lookupLatest((K) key, serializedKey);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
					super.put((K) key, value);
					evictIfFull();
				} else {
					value = Optional.absent();
				}
			}

			return value;
		}

		public Map<K, Optional<V>> getModified() {
			return modified;
		}

		@Override
		protected boolean removeEldestEntry(Entry<K, Optional<V>> eldest) {
			// We need to remove elements manually if the cache becomes full, so
			// we always return false here.
			return false;
		}

		private void evictIfFull() {
			if (size() > cacheSize) {

				LOG.debug("State cache full for {}. Evicting up to {} modified values...", kvState.stateId,
						evictionSize);

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
					if (!toEvict.isEmpty()) {
						kvState.evictModified(toEvict, kvState.lastCheckpointId, kvState.lastCheckpointTs,
								kvState.currentTs);
						kvState.currentTs++;
					}
				} catch (IOException e) {
					throw new RuntimeException(e);
				}

				LOG.debug("Successfully evicted {} modified values for {}.", numEvicted, kvState.stateId);
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
