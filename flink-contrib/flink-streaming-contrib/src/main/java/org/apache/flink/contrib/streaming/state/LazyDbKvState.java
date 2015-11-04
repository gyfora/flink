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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.flink.streaming.api.checkpoint.CheckpointNotifier;
import org.apache.flink.util.InstantiationUtil;
import org.eclipse.jetty.util.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

/**
 * 
 * Lazily fetched {@link KvState} using a SQL backend. Key-value pairs are
 * cached on heap and are lazily retrieved on access.
 * 
 */
public class LazyDbKvState<K, V> implements KvState<K, V, DbStateBackend>, CheckpointNotifier {

	private static final Logger LOG = LoggerFactory.getLogger(LazyDbKvState.class);

	// ------------------------------------------------------

	// Unique id for this state (jobID_operatorID_stateName)
	private final String kvStateId;
	// Only 1 KvState per table will compact
	private final boolean compact;

	private K currentKey;
	private final V defaultValue;

	private final TypeSerializer<K> keySerializer;
	private final TypeSerializer<V> valueSerializer;

	// ------------------------------------------------------

	// Max number of retries for failed database operations
	private final int numSqlRetries;
	// Sleep time between two retries
	private final int sqlRetrySleep;
	// Max number of key-value pairs inserted in one batch to the database
	private final int maxInsertBatchSize;
	// We will do database compaction every so many checkpoints
	private final int compactEvery;

	// Database properties
	private final Connection con;
	private final DbAdapter dbAdapter;

	// Statements for key-lookups and inserts as prepared by the dbAdapter
	private PreparedStatement selectStatement;
	private PreparedStatement insertStatement;

	// ------------------------------------------------------

	// LRU cache for the key-value states backed by the database
	private final StateCache cache;

	// Timestamp of the last written state
	private long lastTimestamp;

	private Map<Long, Long> completedCheckpoints = new HashMap<>();

	// ------------------------------------------------------

	/**
	 * Constructor to initialize the {@link LazyDbKvState} the first time the
	 * job starts.
	 */
	public LazyDbKvState(String kvStateId, boolean compact, Connection con, DbBackendConfig conf,
			TypeSerializer<K> keySerializer,
			TypeSerializer<V> valueSerializer, V defaultValue) throws IOException {
		this(kvStateId, compact, con, conf, keySerializer, valueSerializer, defaultValue, -1);
	}

	/**
	 * Initialize the {@link LazyDbKvState} from a snapshot.
	 */
	public LazyDbKvState(String kvStateId, boolean cleanup, Connection con, final DbBackendConfig conf,
			TypeSerializer<K> keySerializer,
			TypeSerializer<V> valueSerializer, V defaultValue, long lookupTs) throws IOException {

		this.kvStateId = kvStateId;
		this.compact = cleanup;

		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		this.defaultValue = defaultValue;

		this.maxInsertBatchSize = conf.getMaxKvInsertBatchSize();
		this.con = con;
		this.dbAdapter = conf.getDbAdapter();
		this.compactEvery = conf.getKvStateCompactionFrequency();
		this.numSqlRetries = conf.getMaxNumberOfSqlRetries();
		this.sqlRetrySleep = conf.getSleepBetweenSqlRetries();

		this.lastTimestamp = lookupTs;

		this.cache = new StateCache(conf.getKvCacheSize(), conf.getNumElementsToEvict());

		initDB(this.con);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Lazy database kv-state ({}) successfully initialized", kvStateId);
		}
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
	public DbKvStateSnapshot<K, V> shapshot(long checkpointId, long timestamp) throws IOException {

		if (timestamp < lastTimestamp) {
			// This would violate our timing assumptions and break key lookups
			throw new RuntimeException("Checkpoint has lower timestamp than the last written timestamp.");
		}

		// We need to keep track of this to know when to execute the insert
		int rowsInBatchStatement = 0;

		// We insert the cached and modified entries to the database then clear
		// the map of modified entries
		for (Entry<K, Optional<V>> state : cache.modified.entrySet()) {
			rowsInBatchStatement = batchInsert(timestamp, state.getKey(), state.getValue().orNull(),
					rowsInBatchStatement);
		}
		cache.modified.clear();

		// We signal the end of the batch to flush the remaining inserts
		if (rowsInBatchStatement > 0) {
			batchInsert(0, null, null, 0);
		}

		// Update the last timestamp
		lastTimestamp = timestamp;

		completedCheckpoints.put(checkpointId, timestamp);
		return new DbKvStateSnapshot<K, V>(kvStateId, timestamp);
	}

	/**
	 * Returns the number of elements currently stored in the task's cache. Note
	 * that the number of elements in the database is not counted here.
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

		retry(new Callable<Void>() {
			public Void call() throws Exception {

				dbAdapter.createKVStateTable(kvStateId, con);

				insertStatement = dbAdapter.prepareKVCheckpointInsert(kvStateId, con);
				selectStatement = dbAdapter.prepareKeyLookup(kvStateId, con);

				return null;
			}

		}, numSqlRetries, sqlRetrySleep);
	}

	/**
	 * This method inserts checkpoint entries into the database by batching them
	 * up and executing a batchUpdate if it fills up. We can additionally
	 * "flush" the batch contents passing null as key.
	 * 
	 * @return The current number of inserts in the batch statement
	 */
	private int batchInsert(final long timestamp, final K key, final V value, final int batchCount) throws IOException {

		return retry(new Callable<Integer>() {
			public Integer call() throws Exception {
				if (key != null) {
					// We set the insert statement parameters then add it to the
					// current batch of inserts
					dbAdapter.setKVCheckpointInsertParams(kvStateId, insertStatement, timestamp,
							InstantiationUtil.serializeToByteArray(keySerializer, key),
							value != null ? InstantiationUtil.serializeToByteArray(valueSerializer, value) : null);
					insertStatement.addBatch();
				}

				// If the batch is full we execute it
				if (batchCount + 1 >= maxInsertBatchSize || key == null) {
					// Since we cannot reinsert the same values after a
					// failure during the batch insert, we need to make
					// it one transaction
					con.setAutoCommit(false);
					insertStatement.executeBatch();
					con.commit();

					// If the commit was successful we clear the batch an turn
					// autocommit back for the connection so lookups don't need
					// to commit
					insertStatement.clearBatch();
					con.setAutoCommit(true);
					if (Log.isDebugEnabled()) {
						Log.debug("Written {} records to the database for state {}.", batchCount, kvStateId);
					}
					return 0;
				} else {
					return batchCount + 1;
				}
			}
		}, new Callable<Void>() {
			// When the retrier get's an exception during the batch insert it
			// should roll back the transaction before trying it again
			public Void call() throws Exception {
				con.rollback();
				return null;
			}
		}, numSqlRetries, sqlRetrySleep);
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		// We fetch the corresponding timestamps for compaction
		long checkpointTs = completedCheckpoints.remove(checkpointId);
		// If compaction is turned on we compact on the first subtask
		if (compactEvery > 0 && compact && checkpointId % compactEvery == 0) {
			dbAdapter.compactKvStates(kvStateId, con, 0, checkpointTs);
			if (LOG.isDebugEnabled()) {
				LOG.debug("State succesfully compacted for {}.", kvStateId);
			}
		}
	}

	@Override
	public void dispose() {
		// We are only closing the statements here, the connection is borrowed
		// from the state backend and will be closed there.
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

	/**
	 * Return the Map of cached states.
	 * 
	 */
	public Map<K, Optional<V>> getStateCache() {
		return cache;
	}

	/**
	 * Return the Map of modified states that hasn't been written to the
	 * database yet.
	 * 
	 */
	public Map<K, Optional<V>> getModified() {
		return cache.modified;
	}
	
	public boolean isCompacter(){
		return compact;
	}

	/**
	 * Snapshot that stores a specific checkpoint timestamp and state id, and
	 * also rolls back the database to that point upon restore. The rollback is
	 * done by removing all state checkpoints that have timestamps between the
	 * checkpoint and recovery timestamp.
	 *
	 */
	private static class DbKvStateSnapshot<K, V> implements KvStateSnapshot<K, V, DbStateBackend> {

		private static final long serialVersionUID = 1L;

		private final String kvStateId;
		private final long checkpointTimestamp;

		public DbKvStateSnapshot(String kvStateId, long checkpointTimestamp) {
			this.checkpointTimestamp = checkpointTimestamp;
			this.kvStateId = kvStateId;
		}

		@Override
		public LazyDbKvState<K, V> restoreState(final DbStateBackend stateBackend,
				final TypeSerializer<K> keySerializer, final TypeSerializer<V> valueSerializer, final V defaultValue,
				ClassLoader classLoader, final long recoveryTimestamp) throws IOException {

			// Validate our timing assumptions
			if (recoveryTimestamp < checkpointTimestamp) {
				throw new RuntimeException("Recovery timestamp must be greater than the last checkpoint timestamp");
			}

			// First we clean up the states written by partially failed
			// snapshots
			retry(new Callable<Void>() {
				public Void call() throws Exception {
					stateBackend.getConfiguration().getDbAdapter().cleanupFailedCheckpoints(kvStateId,
							stateBackend.getConnection(), checkpointTimestamp, recoveryTimestamp);

					return null;
				}
			}, stateBackend.getConfiguration().getMaxNumberOfSqlRetries(),
					stateBackend.getConfiguration().getSleepBetweenSqlRetries());
			
			boolean cleanup = stateBackend.getEnvironment().getIndexInSubtaskGroup() == stateBackend.getShardIndex();

			// Restore the KvState
			LazyDbKvState<K, V> restored = new LazyDbKvState<K, V>(kvStateId, cleanup,
					stateBackend.getConnection(), stateBackend.getConfiguration(), keySerializer, valueSerializer,
					defaultValue, checkpointTimestamp);

			if (LOG.isDebugEnabled()) {
				LOG.debug("KV state({},{}) restored.", kvStateId, checkpointTimestamp);
			}

			return restored;
		}

		@Override
		public void discardState() throws Exception {
			// Don't discard, it will be compacted by the LazyDbKvState
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
	private final class StateCache extends LinkedHashMap<K, Optional<V>> {
		private static final long serialVersionUID = 1L;

		private final int cacheSize;
		private final int evictionSize;

		// We keep track the state modified since the last checkpoint
		private final Map<K, Optional<V>> modified = new HashMap<>();

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
				// If it doesn't try to load it from the database
				value = Optional.fromNullable(getFromDatabaseOrNull((K) key));
				put((K) key, value);
			}
			// We currently mark elements that were retreived also as modified
			// in case the user applies some mutation without update.
			modified.put((K) key, value);
			return value;
		}

		@Override
		protected boolean removeEldestEntry(Entry<K, Optional<V>> eldest) {
			// We need to remove elements manually if the cache becomes full, so
			// we always return false here.
			return false;
		}

		/**
		 * Fetch the current value from the database if exists or return null.
		 * 
		 * @param key
		 * @return The value corresponding to the key and the last timestamp
		 *         from the database if exists or null.
		 */
		private V getFromDatabaseOrNull(final K key) {
			try {
				return retry(new Callable<V>() {
					public V call() throws Exception {
						// We lookup using the adapter and serialize/deserialize
						// with the TypeSerializers
						byte[] serializedVal = dbAdapter.lookupKey(kvStateId, selectStatement,
								InstantiationUtil.serializeToByteArray(keySerializer, key), lastTimestamp);

						return serializedVal != null
								? InstantiationUtil.deserializeFromByteArray(valueSerializer, serializedVal) : null;
					}
				}, numSqlRetries, sqlRetrySleep);
			} catch (IOException e) {
				// We need to re-throw this exception to conform to the map
				// interface, we will catch this when we call the the put/get
				throw new RuntimeException(e);
			}
		}

		/**
		 * If the cache is full we remove the evictionSize least recently
		 * accessed elements and write them to the database if they were
		 * modified since the last checkpoint.
		 */
		private void evictIfFull() {
			if (size() > cacheSize) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("State cache is full for {}, evicting {} elements.", kvStateId, evictionSize);
				}
				try {
					int numEvicted = 0;
					int rowsInBatch = 0;
					boolean writtenToDb = false;

					Iterator<Entry<K, Optional<V>>> entryIterator = entrySet().iterator();
					while (numEvicted++ < evictionSize && entryIterator.hasNext()) {

						Entry<K, Optional<V>> next = entryIterator.next();

						// We only need to write to the database if modified
						if (modified.remove(next.getKey()) != null) {
							// We insert elements with timestamp + 1 to ensure
							// good lookups. This won't interfere with the
							// actual checkpoint timestamps as the next one
							// should be much higher.
							rowsInBatch = batchInsert(lastTimestamp + 1, next.getKey(), next.getValue().orNull(),
									numEvicted);
							if (rowsInBatch == 0) {
								writtenToDb = true;
							}
						}

						entryIterator.remove();
					}
					// Flush batch if has rows
					if (rowsInBatch > 0) {
						batchInsert(0, null, null, 0);
						writtenToDb = true;
					}

					// If we have written new values to the database we need to
					// set the lastTimestamp accordingly
					if (writtenToDb) {
						lastTimestamp = lastTimestamp + 1;
					}

				} catch (IOException e) {
					// We need to re-throw this exception to conform to the map
					// interface, we will catch this when we call the the
					// put/get
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
