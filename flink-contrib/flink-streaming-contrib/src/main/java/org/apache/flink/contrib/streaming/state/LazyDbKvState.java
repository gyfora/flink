/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import static org.apache.flink.contrib.streaming.state.SQLRetrier.retry;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.flink.util.InstantiationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

/**
 * 
 * Lazily fetched {@link KvState} using a SQL backend. Key-value pairs are
 * cached on heap and are lazily retrieved on access. Key's that are dropped
 * from the cache will remain on heap until they are evicted to the backend upon
 * the next successful checkpoint.
 * 
 */
public class LazyDbKvState<K, V> implements KvState<K, V, DbStateBackend> {

	private static final Logger LOG = LoggerFactory.getLogger(LazyDbKvState.class);

	// ------------------------------------------------------

	// Unique id for this state (jobID_operatorID_stateName)
	private final String kvStateId;

	private K currentKey;
	private final V defaultValue;

	private final TypeSerializer<K> keySerializer;
	private final TypeSerializer<V> valueSerializer;

	// ------------------------------------------------------

	// Max number of retries for failed database operations
	private static final int NUM_RETRIES = 5;
	// Max number of key-value pairs inserted in one batch to the database
	private final int maxInsertBatchSize;

	private final Connection con;
	private final DbAdapter dbAdapter;

	private PreparedStatement selectStatement;
	private PreparedStatement insertStatement;

	// ------------------------------------------------------

	// LRU cache for the key-value states backed by the database
	private final StateCache cache;

	// Checkpoint ID and timestamp of the last successful checkpoint for loading
	// missing values to the cache
	private long lookupId;
	private long lookupTs;

	// ------------------------------------------------------

	public LazyDbKvState(String kvStateId, Connection con, DbBackendConfig conf, TypeSerializer<K> keySerializer,
			TypeSerializer<V> valueSerializer, V defaultValue) throws IOException {
		this(kvStateId, con, conf, keySerializer, valueSerializer, defaultValue, -1, -1);
	}

	public LazyDbKvState(String kvStateId, Connection con, final DbBackendConfig conf, TypeSerializer<K> keySerializer,
			TypeSerializer<V> valueSerializer, V defaultValue, long lookupId, long lookupTs) throws IOException {

		this.kvStateId = kvStateId;

		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		this.defaultValue = defaultValue;

		this.lookupId = lookupId;
		this.lookupTs = lookupTs;

		this.cache = new StateCache(conf.getKvCacheSize(), conf.getNumElementsToEvict());
		this.maxInsertBatchSize = conf.getMaxKvInsertBatchSize();
		this.con = con;
		this.dbAdapter = conf.getDbAdapter();

		initDB(this.con);
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
			throw new IOException(e);
		}
	}

	@Override
	public V value() throws IOException {
		try {
			// We get the value from the cache (or load from the database). If
			// null, we return a copy of the default value
			V val = cache.get(currentKey).orNull();
			return val != null ? val : copyDefault();
		} catch (RuntimeException e) {
			throw new IOException(e);
		}
	}

	@Override
	public DbKvStateSnapshot<K, V> shapshot(long checkpointId, long timestamp) throws IOException {

		int rowsInBatchStatement = 0;
		int sizeBeforeSnapshot = size();

		// We write the cached entries to the database
		for (Entry<K, Optional<V>> state : cache.entrySet()) {
			rowsInBatchStatement = batchInsert(checkpointId, timestamp, state.getKey(), state.getValue().orNull(),
					rowsInBatchStatement);
		}

		// We signal the end of the batch to flush the remaining inserts
		if (rowsInBatchStatement > 0) {
			batchInsert(0, 0, null, null, 0);
		}

		// Update the lookup id and timestamp so future cache loads will return
		// the checkpointed values
		lookupId = checkpointId;
		lookupTs = timestamp;

		if (LOG.isDebugEnabled()) {
			LOG.debug("Snapshot taken for {} kv pairs.", sizeBeforeSnapshot);
		}
		return new DbKvStateSnapshot<K, V>(kvStateId, checkpointId, timestamp);
	}

	/**
	 * Returns the number of elements currently stored in the task (cache + cold
	 * states). Note that the number of elements in the database is not counted
	 * here.
	 */
	@Override
	public int size() {
		return cache.size();
	}

	/**
	 * Return a copy the default value or null if the default was null.
	 * 
	 */
	private V copyDefault() {
		return defaultValue != null ? valueSerializer.copy(defaultValue) : null;
	}

	/**
	 * Create a table for the kvstate checkpoints (based on the kvStateId) and
	 * prepare the statements used during checkpointing.
	 */
	private void initDB(final Connection con) throws IOException {

		retry(new Callable<Boolean>() {
			public Boolean call() throws Exception {

				dbAdapter.createKVStateTable(kvStateId, con);

				insertStatement = dbAdapter.prepareKVCheckpointInsert(kvStateId, con);
				selectStatement = dbAdapter.prepareKeyLookup(kvStateId, con);

				return true;
			}

		}, NUM_RETRIES);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Lazy database kv-state ({}) successfully initialized", kvStateId);
		}
	}

	/**
	 * This method inserts checkpoint entries into the database by batching them
	 * up and executing a batchUpdate if it fills up. We can additionally
	 * "flush" the batch contents passing null as key.
	 */
	private int batchInsert(final long checkpointId, final long timestamp, final K key, final V value,
			final int batchCount)
					throws IOException {

		return retry(new Callable<Integer>() {
			public Integer call() throws Exception {
				if (key != null) {
					dbAdapter.setKVCheckpointInsertParams(kvStateId, insertStatement, checkpointId, timestamp,
							InstantiationUtil.serializeToByteArray(keySerializer, key),
							value != null ? InstantiationUtil.serializeToByteArray(valueSerializer, value) : null);
					insertStatement.addBatch();
				}

				if (batchCount + 1 == maxInsertBatchSize || key == null) {
					// Since we cannot reinsert the same values after a
					// failure during the batch insert we need to make
					// it one transaction
					con.setAutoCommit(false);
					insertStatement.executeBatch();
					con.commit();
					insertStatement.clearBatch();
					con.setAutoCommit(true);
					return 0;
				} else {
					return batchCount + 1;
				}
			}
		}, new Callable<Boolean>() {
			public Boolean call() throws Exception {
				con.rollback();
				return true;
			}
		}, NUM_RETRIES);
	}

	@Override
	public void dispose() {
		try {
			selectStatement.close();
		} catch (SQLException e) {
			// There is not much to do about this
		}
		try {
			insertStatement.close();
		} catch (SQLException e) {
			// There is not much to do about this
		}
	}

	public Map<K, Optional<V>> getStateCache() {
		return cache;
	}

	/**
	 * 
	 * Snapshot that stores a specific lookup checkpoint id and timestamp, and
	 * also rolls back the database to that point upon restore. The rollback is
	 * done by removing all state checkpoints that have larger id than the
	 * lookup id and smaller timestamp than the recovery timestamp.
	 *
	 */
	private static class DbKvStateSnapshot<K, V> implements KvStateSnapshot<K, V, DbStateBackend> {

		private static final long serialVersionUID = 1L;

		private final String kvStateId;

		private final long lookupId;
		private final long lookupTs;

		public DbKvStateSnapshot(String kvStateId, long lookupId, long lookupTs) {
			this.lookupId = lookupId;
			this.lookupTs = lookupTs;
			this.kvStateId = kvStateId;
		}

		@Override
		public LazyDbKvState<K, V> restoreState(final DbStateBackend stateBackend,
				final TypeSerializer<K> keySerializer,
				final TypeSerializer<V> valueSerializer, 
				final V defaultValue, 
				ClassLoader classLoader,
				final long recoveryTimestamp)
						throws IOException {

			return retry(new Callable<LazyDbKvState<K, V>>() {
				public LazyDbKvState<K, V> call() throws Exception {

					stateBackend.getConfiguration().getDbAdapter().cleanupFailedCheckpoints(kvStateId,
							stateBackend.getConnection(),
							lookupId, recoveryTimestamp);

					if (LOG.isDebugEnabled()) {
						LOG.debug("KV state({},{}) restored.", lookupId, lookupTs);
					}
					return new LazyDbKvState<K, V>(kvStateId, stateBackend.getConnection(),
							stateBackend.getConfiguration(), keySerializer, valueSerializer,
							defaultValue, lookupId, lookupTs);
				}

			}, NUM_RETRIES);
		}

		@Override
		public void discardState() throws Exception {
			// TODO: Consider compaction at this point
		}

	}

	/**
	 * LRU cache implementation for storing the key-value states. When the cache
	 * is full elements are not evicted one by one but are evicted in a batch
	 * defined by the evictionSize parameter.
	 * <p>
	 * Keys not found in the cached will be retrieved from the underlying
	 * database
	 */
	private class StateCache extends LinkedHashMap<K, Optional<V>> {
		private static final long serialVersionUID = 1L;
		private final int cacheSize;
		private final int evictionSize;

		public StateCache(int cacheSize, int evictionSize) {
			super(cacheSize, 0.75f, true);
			this.cacheSize = cacheSize;
			this.evictionSize = evictionSize;
		}

		@Override
		public Optional<V> put(K key, Optional<V> value) {
			// Put kv pair in the cache and evict elements if the cache is full
			Optional<V> out = super.put(key, value);
			evictIfFull();
			return out;
		}

		@SuppressWarnings("unchecked")
		@Override
		public Optional<V> get(Object key) {
			Optional<V> value = super.get(key);
			if (value == null) {
				// Load into cache from database if exists
				value = Optional.fromNullable(getFromDatabaseOrNull((K) key));
				put((K) key, value);
			}

			return value;
		}

		@Override
		protected boolean removeEldestEntry(Entry<K, Optional<V>> eldest) {
			// We remove manually
			return false;
		}

		/**
		 * Fetch the current value from the database if exists or return null.
		 * 
		 * @param key
		 * @return The value corresponding to the lookupId and lookupTs from the
		 *         database if exists or null.
		 */
		private V getFromDatabaseOrNull(final K key) {
			try {
				return retry(new Callable<V>() {
					public V call() throws Exception {
						byte[] serializedVal = dbAdapter.lookupKey(kvStateId, selectStatement,
								InstantiationUtil.serializeToByteArray(keySerializer, key), lookupId, lookupTs);

						return serializedVal != null
								? InstantiationUtil.deserializeFromByteArray(valueSerializer, serializedVal) : null;
					}
				}, NUM_RETRIES);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		/**
		 * If the cache is full the evictionSize least recently accessed
		 * elements will be removed from the cache.
		 */
		private void evictIfFull() {
			if (size() > cacheSize) {
				try {
					int numEvicted = 0;
					int rowsInBatch = 0;
					Iterator<Entry<K, Optional<V>>> entryIterator = entrySet().iterator();
					while (numEvicted < evictionSize && entryIterator.hasNext()) {
						Entry<K, Optional<V>> next = entryIterator.next();
						rowsInBatch = batchInsert(lookupId, lookupTs + 1, next.getKey(), next.getValue().orNull(),
								numEvicted);
						entryIterator.remove();
						numEvicted++;
					}
					if (rowsInBatch > 0) {
						batchInsert(0, 0, null, null, 0);
					}
					lookupTs = lookupTs + 1;
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}

		@Override
		public void putAll(Map<? extends K, ? extends Optional<V>> m) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void clear() {
			throw new UnsupportedOperationException();
		}
	}

}
