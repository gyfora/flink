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

package org.apache.flink.contrib.streaming.state.hdfs;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.OutOfCoreKvState;
import org.apache.flink.contrib.streaming.state.hdfs.HdfsCheckpointManager.Interval;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.util.InstantiationUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

public class HdfsKvState<K, V> extends OutOfCoreKvState<K, V, FsStateBackend> {

	private static final Logger LOG = LoggerFactory.getLogger(HdfsKvState.class);

	private final Path cpParentDir;
	private final HdfsCheckpointManager checkpointManager;
	private final ExecutorService executor;

	public HdfsKvState(
			HdfsKvStateConfig kvStateConf,
			TypeSerializer<K> keySerializer,
			TypeSerializer<V> valueSerializer,
			V defaultValue,
			long lastCheckpointTs,
			long currentTs,
			FileSystem fs,
			Path cpParentDir,
			Map<Interval, Path> cpFiles) {

		super(kvStateConf, keySerializer, valueSerializer, defaultValue, 0,
				lastCheckpointTs, currentTs);

		this.cpParentDir = cpParentDir;

		this.checkpointManager = new HdfsCheckpointManager(fs, cpParentDir, cpFiles, kvStateConf);
		this.executor = Executors.newSingleThreadExecutor();
	}

	@Override
	public void snapshotModified(Collection<Entry<K, Optional<V>>> modifiedKVs, long checkpointId,
			final long timestamp) {

		Tuple2<Double, Double> kbytesWritten = checkpointManager.snapshot(modifiedKVs, timestamp, keySerializer,
				valueSerializer);

		LOG.debug("Successfully written checkpoint to disk - (keyKBytes, valueKBytes) = {}", kbytesWritten);

		executor.execute(new Runnable() {

			@Override
			public void run() {
				try {
					checkpointManager.mergeAndRemove(lastCheckpointTs, timestamp);
					LOG.info("Succesfully compacted states between: {} and {}", lastCheckpointTs + 1, timestamp);
				} catch (IOException e) {
					LOG.info("Error while compacting states: {}", e.getMessage());
				}
			}
		});
	}

	@Override
	public void evictModified(Collection<Entry<K, Optional<V>>> KVsToEvict, long lastCheckpointId,
			long lastCheckpointTs, long currentTs) throws IOException {

		Tuple2<Double, Double> kbytesWritten = checkpointManager.snapshot(KVsToEvict, currentTs, keySerializer,
				valueSerializer);
		LOG.debug("Successfully evicted state to disk - (keyKBytes, valueKBytes) = {}", kbytesWritten);
	}

	@Override
	public Optional<V> lookupLatest(K key) {
		try {
			final byte[] serializedKey = InstantiationUtil.serializeToByteArray(keySerializer, key);

			return checkpointManager.lookupKey(serializedKey, valueSerializer);
		} catch (IOException e) {
			// We need to re-throw this exception to conform to the map
			// interface, we will catch this when we call the the put/get
			throw new RuntimeException("Could not get state for key: " + key, e);
		}
	}

	@Override
	public KvStateSnapshot<K, V, FsStateBackend> createSnapshot(long checkpointId, long timestamp) {
		return new HdfsKvStateSnapshot<K, V>(timestamp, cpParentDir, checkpointManager.getIntervalMapping());
	}

	public HdfsCheckpointManager getKeyScanner() {
		return checkpointManager;
	}

	@Override
	public int size() {
		return 0;
	}

	@Override
	public void dispose() {
		try {
			checkpointManager.close();
		} catch (Exception e) {
		}
	}

	private static class HdfsKvStateSnapshot<K, V> implements KvStateSnapshot<K, V, FsStateBackend> {

		private static final long serialVersionUID = 1L;

		private long timestamp;
		private URI cpParentDir;
		private Map<Interval, URI> paths;

		public HdfsKvStateSnapshot(long timestamp, Path cpParentDir, Map<Interval, Path> paths) {
			this.timestamp = timestamp;
			this.cpParentDir = cpParentDir.toUri();
			this.paths = new HashMap<>();
			for (Entry<Interval, Path> e : paths.entrySet()) {
				this.paths.put(e.getKey(), e.getValue().toUri());
			}
		}

		@Override
		public KvState<K, V, FsStateBackend> restoreState(FsStateBackend stateBackend, TypeSerializer<K> keySerializer,
				TypeSerializer<V> valueSerializer, V defaultValue, ClassLoader classLoader, long recoveryTimestamp)
						throws Exception {

			HdfsStateBackend backend = (HdfsStateBackend) stateBackend;

			Map<Interval, Path> restoredPaths = new HashMap<>();
			for (Entry<Interval, URI> e : paths.entrySet()) {
				restoredPaths.put(e.getKey(), new Path(e.getValue()));
			}

			return new HdfsKvState<>(backend.getKvStateConf(), keySerializer, valueSerializer,
					defaultValue, timestamp, timestamp + 1, backend.getHadoopFileSystem(), new Path(cpParentDir),
					restoredPaths);
		}

		@Override
		public void discardState() throws Exception {
			// Remove files?
		}

		@Override
		public long getStateSize() throws Exception {
			return 0;
		}

	}
}
