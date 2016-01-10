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
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.ShardedConnection.ShardedStatement;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.flink.runtime.state.StateBackend.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.streaming.api.checkpoint.CheckpointNotifier;
import org.apache.flink.util.InstantiationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.hash.BloomFilter;

/**
 * 
 * Lazily fetched {@link KvState} using a SQL backend. Key-value pairs are
 * cached on heap and are lazily retrieved on access.
 * 
 */
public class DbKvState<K, V> extends OutOfCoreKvState<K, V, DbStateBackend> implements CheckpointNotifier {

	private static final Logger LOG = LoggerFactory.getLogger(DbKvState.class);

	// ------------------------------------------------------

	private final DbStateBackend backend;

	// Unique id for this state (appID_operatorID_stateName)
	private final String kvStateId;
	private final boolean compact;

	// ------------------------------------------------------

	// Max number of retries for failed database operations
	private final int numSqlRetries;
	// Sleep time between two retries
	private final int sqlRetrySleep;
	// Max number of key-value pairs inserted in one batch to the database
	private final int maxInsertBatchSize;
	// We will do database compaction every so many checkpoints
	private final int compactEvery;
	// Executor for automatic compactions
	private ExecutorService executor = null;

	// Database properties
	private final DbBackendConfig conf;
	private final ShardedConnection connections;
	private final DbAdapter dbAdapter;

	// Convenience object for handling inserts to the database
	private final BatchInserter batchInsert;

	// Statements for key-lookups and inserts as prepared by the dbAdapter
	private ShardedStatement selectStatements;
	private ShardedStatement insertStatements;

	// ------------------------------------------------------

	private Map<Long, Long> completedCheckpoints = new HashMap<>();

	private volatile long lastCompactedTs;

	private BloomFilter<byte[]> bloomFilter = null;

	long lookupTime = 0;
	long lookupCount = 0;
	// ------------------------------------------------------

	/**
	 * Constructor to initialize the {@link DbKvState} the first time the job
	 * starts.
	 */
	public DbKvState(DbStateBackend backend, String kvStateId, boolean compact, ShardedConnection cons,
			DbBackendConfig conf,
			TypeSerializer<K> keySerializer, TypeSerializer<V> valueSerializer, V defaultValue) throws IOException {
		this(backend, kvStateId, compact, cons, conf, keySerializer, valueSerializer, defaultValue, 1, 0, null);
	}

	/**
	 * Initialize the {@link DbKvState} from a snapshot.
	 */
	public DbKvState(DbStateBackend backend, String kvStateId, boolean compact, ShardedConnection cons,
			final DbBackendConfig conf,
			TypeSerializer<K> keySerializer, TypeSerializer<V> valueSerializer, V defaultValue, long nextTs,
			long lastCompactedTs, BloomFilter<byte[]> restoredFilter)
					throws IOException {

		super(conf, keySerializer, valueSerializer, defaultValue, 0, 0, nextTs);

		this.backend = backend;

		this.kvStateId = kvStateId;
		this.compact = compact;
		if (compact) {
			// Compactions will run in a seperate thread
			executor = Executors.newSingleThreadExecutor();
		}

		this.maxInsertBatchSize = conf.getMaxKvInsertBatchSize();
		this.conf = conf;
		this.connections = cons;
		this.dbAdapter = conf.getDbAdapter();
		this.compactEvery = conf.getKvStateCompactionFrequency();
		this.numSqlRetries = conf.getMaxNumberOfSqlRetries();
		this.sqlRetrySleep = conf.getSleepBetweenSqlRetries();

		this.lastCompactedTs = lastCompactedTs;

		initDB(this.connections);

		batchInsert = new BatchInserter(connections.getNumShards());

		// If the kvstate is restored from a checkpoint that had a bloomfilter
		// we use that, otherwise create (or not) based in the config
		if (restoredFilter != null && conf.hasBloomFilter()) {
			bloomFilter = restoredFilter;
		} else if (conf.hasBloomFilter()) {
			bloomFilter = BloomFilter.create(new KeyFunnel(), conf.getBloomFilterExpectedInserts(),
					conf.getBloomFilterFPP());

			LOG.debug("Bloomfilter created for kv-state {} with fpp={} for expectedInserts={}", kvStateId,
					conf.getBloomFilterFPP(), conf.getBloomFilterExpectedInserts());
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Lazy database kv-state ({}) successfully initialized", kvStateId);
		}
	}

	@Override
	public void snapshotModified(Collection<Entry<K, Optional<V>>> modifiedKVs, long checkpointId, long timestamp)
			throws IOException {
		for (Entry<K, Optional<V>> state : modifiedKVs) {
			batchInsert.add(state, timestamp);
		}
		batchInsert.flush(timestamp);
	}

	@Override
	public KvStateSnapshot<K, V, DbStateBackend> snapshot(long checkpointId, long timestamp) throws Exception {

		KvStateSnapshot<K, V, DbStateBackend> cp = super.snapshot(checkpointId, timestamp);

		if (compact) {
			// Otherwise we call the keep alive method to avoid dropped
			// connections (only call this on the compactor instance)
			for (final Connection c : connections.connections()) {
				SQLRetrier.retry(new Callable<Void>() {
					@Override
					public Void call() throws Exception {
						dbAdapter.keepAlive(c);
						return null;
					}
				}, numSqlRetries, sqlRetrySleep);
			}
		}

		completedCheckpoints.put(checkpointId, timestamp);

		return cp;
	}

	@Override
	public KvStateSnapshot<K, V, DbStateBackend> createSnapshot(long checkpointId, long timestamp) throws Exception {
		// If the bloomfilter is enabled we need to checkpoint that
		StreamStateHandle filterCheckpoint = null;

		if (bloomFilter != null) {
			// We use the efficient serialization of the bloomfilter instead of
			// Java serialization
			CheckpointStateOutputStream cpStream = backend.createCheckpointStateOutputStream(checkpointId, timestamp);
			bloomFilter.writeTo(cpStream);
			filterCheckpoint = cpStream.closeAndGetHandle();
		}

		return new DbKvStateSnapshot<K, V>(kvStateId, timestamp, lastCompactedTs, filterCheckpoint);
	}

	@Override
	public Optional<V> lookupLatest(final K key) {
		try {
			final byte[] serializedKey = InstantiationUtil.serializeToByteArray(keySerializer, key);

			boolean dbMightContain = bloomFilter == null || !bloomFilter.put(serializedKey);

			if (dbMightContain) {
				return retry(new Callable<Optional<V>>() {
					public Optional<V> call() throws Exception {
						// We lookup using the adapter and
						// serialize/deserialize
						// with the TypeSerializers
						long start = System.nanoTime();

						byte[] serializedVal = dbAdapter.lookupKey(kvStateId,
								selectStatements.getForKey(key), serializedKey, currentTs);

						if (LOG.isDebugEnabled()) {
							lookupTime += (System.nanoTime() - start) / 1000000;
							lookupCount++;

							if (lookupCount % 10000 == 0) {
								LOG.debug("Total lookup time (numRecords, lookupTime(ms)): ({},{})",
										lookupCount, lookupTime);
								lookupCount = 0;
								lookupTime = 0;
							}
						}

						return serializedVal != null
								? Optional
										.of(InstantiationUtil.deserializeFromByteArray(valueSerializer, serializedVal))
								: Optional.<V> absent();
					}
				}, numSqlRetries, sqlRetrySleep);
			} else {
				return Optional.absent();
			}

		} catch (Exception e) {
			// We need to re-throw this exception to conform to the map
			// interface, we will catch this when we call the the put/get
			throw new RuntimeException("Could not get state for key: " + key + " from the database.", e);
		}
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
	 * Create a table for the kvstate checkpoints (based on the kvStateId) and
	 * prepare the statements used during checkpointing.
	 */
	private void initDB(final ShardedConnection cons) throws IOException {

		retry(new Callable<Void>() {
			public Void call() throws Exception {

				for (Connection con : cons.connections()) {
					dbAdapter.createKVStateTable(kvStateId, con);
				}

				insertStatements = cons.prepareStatement(dbAdapter.prepareKVCheckpointInsert(kvStateId));
				selectStatements = cons.prepareStatement(dbAdapter.prepareKeyLookup(kvStateId));

				return null;
			}

		}, numSqlRetries, sqlRetrySleep);
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) {
		final Long ts = completedCheckpoints.remove(checkpointId);
		if (ts == null) {
			LOG.warn("Complete notification for missing checkpoint: " + checkpointId);
		} else {
			// If compaction is turned on we compact on the compactor subtask
			// asynchronously in the background
			if (compactEvery > 0 && compact && checkpointId % compactEvery == 0) {
				executor.execute(new Compactor(ts));
			}
		}
	}

	@Override
	public void dispose() {
		// We are only closing the statements here, the connection is borrowed
		// from the state backend and will be closed there.
		try {
			selectStatements.close();
		} catch (SQLException e) {
			// There is not much to do about this
		}
		try {
			insertStatements.close();
		} catch (SQLException e) {
			// There is not much to do about this
		}

		if (executor != null) {
			executor.shutdown();
		}
	}

	/**
	 * Return the Map of modified states that hasn't been written to the
	 * database yet.
	 * 
	 */
	public Map<K, Optional<V>> getModified() {
		return cache.modified;
	}

	/**
	 * Used for testing purposes
	 */
	public boolean isCompactor() {
		return compact;
	}

	/**
	 * Used for testing purposes
	 */
	public ExecutorService getExecutor() {
		return executor;
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
		private final long lastCompactedTimestamp;

		// This field might be null if the state has no bloomfilter
		private final StreamStateHandle bloomFilterHandle;

		public DbKvStateSnapshot(String kvStateId, long checkpointTimestamp, long lastCompactedTs,
				StreamStateHandle bloomFilterHandle) {
			this.checkpointTimestamp = checkpointTimestamp;
			this.kvStateId = kvStateId;
			this.lastCompactedTimestamp = lastCompactedTs;
			this.bloomFilterHandle = bloomFilterHandle;
		}

		@Override
		public DbKvState<K, V> restoreState(final DbStateBackend stateBackend,
				final TypeSerializer<K> keySerializer, final TypeSerializer<V> valueSerializer, final V defaultValue,
				ClassLoader cl, final long recoveryTimestamp) throws Exception {

			// Validate timing assumptions
			if (recoveryTimestamp <= checkpointTimestamp) {
				throw new RuntimeException(
						"Recovery timestamp is smaller or equal to checkpoint timestamp. "
								+ "This might happen if the job was started with a new JobManager "
								+ "and the clocks got really out of sync.");
			}

			// First we clean up the states written by partially failed
			// snapshots
			retry(new Callable<Void>() {
				public Void call() throws Exception {

					// We need to perform cleanup on all shards to be safe here
					for (Connection c : stateBackend.getConnections().connections()) {
						stateBackend.getConfiguration().getDbAdapter().cleanupFailedCheckpoints(
								stateBackend.getConfiguration(), kvStateId,
								c, checkpointTimestamp, recoveryTimestamp);
					}

					return null;
				}
			}, stateBackend.getConfiguration().getMaxNumberOfSqlRetries(),
					stateBackend.getConfiguration().getSleepBetweenSqlRetries());

			boolean compactor = stateBackend.getEnvironment().getTaskInfo().getIndexOfThisSubtask() == 0;

			// If we checkpointed the bloomfilter we restore it from the input
			// stream
			BloomFilter<byte[]> restoredFilter = null;
			if (bloomFilterHandle != null) {
				InputStream in = bloomFilterHandle.getState(cl);
				restoredFilter = BloomFilter.readFrom(in, new KeyFunnel());
				LOG.debug("Restored bloomfilter for {}", kvStateId);
			}

			// Restore the KvState
			DbKvState<K, V> restored = new DbKvState<K, V>(stateBackend, kvStateId, compactor,
					stateBackend.getConnections(), stateBackend.getConfiguration(), keySerializer, valueSerializer,
					defaultValue, recoveryTimestamp, lastCompactedTimestamp, restoredFilter);

			if (LOG.isDebugEnabled()) {
				LOG.debug("KV state({},{}) restored.", kvStateId, recoveryTimestamp);
			}

			return restored;
		}

		@Override
		public void discardState() throws Exception {
			// Don't discard, it will be compacted by the LazyDbKvState
		}

		@Override
		public long getStateSize() throws Exception {
			// Because the state is serialzied in a lazy fashion we don't know
			// the size of the state yet.
			return 0;
		}

	}

	/**
	 * Object for handling inserts to the database by batching them together
	 * partitioned on the sharding key. The batches are written to the database
	 * when they are full or when the inserter is flushed.
	 *
	 */
	private class BatchInserter {

		// Map from shard index to the kv pairs to be inserted
		// Map<Integer, List<Tuple2<byte[], byte[]>>> inserts = new HashMap<>();

		List<Tuple2<byte[], byte[]>>[] inserts;

		@SuppressWarnings("unchecked")
		public BatchInserter(int numShards) {
			inserts = new List[numShards];
			for (int i = 0; i < numShards; i++) {
				inserts[i] = new ArrayList<>();
			}
		}

		public void add(Entry<K, Optional<V>> next, long timestamp) throws IOException {

			K key = next.getKey();
			V value = next.getValue().orNull();

			// Get the current partition if present or initialize empty list
			int shardIndex = connections.getShardIndex(key);

			List<Tuple2<byte[], byte[]>> insertPartition = inserts[shardIndex];

			// Add the k-v pair to the partition
			byte[] k = InstantiationUtil.serializeToByteArray(keySerializer, key);
			byte[] v = value != null ? InstantiationUtil.serializeToByteArray(valueSerializer, value) : null;
			insertPartition.add(Tuple2.of(k, v));

			// If partition is full write to the database and clear
			if (insertPartition.size() == maxInsertBatchSize) {
				executeInsert(insertPartition, shardIndex, timestamp);
				insertPartition.clear();
			}
		}

		public void flush(long timestamp) throws IOException {
			// We flush all non-empty partitions
			for (int i = 0; i < inserts.length; i++) {
				List<Tuple2<byte[], byte[]>> insertPartition = inserts[i];
				if (!insertPartition.isEmpty()) {
					executeInsert(insertPartition, i, timestamp);
					insertPartition.clear();
				}
			}

		}

		private void executeInsert(List<Tuple2<byte[], byte[]>> kvPairs, int shardIndex, long timestamp)
				throws IOException {

			// Used for debugging purposes
			Long startTime = null;
			if (LOG.isDebugEnabled()) {
				startTime = System.nanoTime();
				LOG.debug("Executing batch state insert for {} shard {} with {} records", kvStateId,
						conf.getShardUrl(shardIndex), kvPairs.size());
			}

			dbAdapter.insertBatch(kvStateId, conf, connections.getForIndex(shardIndex),
					insertStatements.getForIndex(shardIndex), timestamp, kvPairs);

			if (LOG.isDebugEnabled()) {
				long insertTime = (System.nanoTime() - startTime) / 1000000;
				long keySize = 0L;
				long valueSize = 0L;
				for (Tuple2<byte[], byte[]> t : kvPairs) {
					keySize += t.f0.length;
					if (t.f1 != null) {
						valueSize += t.f1.length;
					}
				}
				keySize = keySize / 1024;
				valueSize = valueSize / 1024;
				LOG.debug(
						"Successfully inserted state batch (insertTime(ms), numRecords, totKeySize(kb), totValueSize(kb), stateName, shardUrl, sql): "
								+ "({},{},{},{},{},{},{})",
						insertTime, kvPairs.size(), keySize, valueSize, kvStateId, conf.getShardUrl(shardIndex),
						insertStatements.getSqlString());
			}
		}
	}

	private class Compactor implements Runnable {

		private long upperBound;

		public Compactor(long upperBound) {
			this.upperBound = upperBound;
		}

		@Override
		public void run() {
			// We create new database connections to make sure we don't
			// interfere with the checkpointing (connections are not thread
			// safe)
			try (ShardedConnection sc = conf.createShardedConnection()) {

				LOG.info("Starting compaction for {} between {} and {}.", kvStateId,
						lastCompactedTs,
						upperBound);

				long compactionStart = System.nanoTime();

				for (final Connection c : sc.connections()) {
					SQLRetrier.retry(new Callable<Void>() {
						@Override
						public Void call() throws Exception {
							dbAdapter.compactKvStates(kvStateId, c, lastCompactedTs, upperBound);
							return null;
						}
					}, numSqlRetries, sqlRetrySleep);
				}

				LOG.info("State succesfully compacted for {} between {} and {} in {} ms.", kvStateId,
						lastCompactedTs,
						upperBound,
						(System.nanoTime() - compactionStart) / 1000000);

				lastCompactedTs = upperBound;
			} catch (SQLException | IOException e) {
				LOG.warn("State compaction failed due: {}", e);
			}
		}

	}

}
