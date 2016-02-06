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
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.OutOfCoreKvState;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

public class HdfsKvState<K, N, V> extends OutOfCoreKvState<K, N, V, FsStateBackend> {

	private final HdfsCheckpointManager checkpointManager;

	private static final Logger LOG = LoggerFactory.getLogger(HdfsKvState.class);
	protected final ExecutorService executor = Executors.newSingleThreadExecutor();

	public HdfsKvState(
			FsStateBackend backend,
			String stateId,
			HdfsKvStateConfig kvStateConf,
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			ValueStateDescriptor<V> stateDesc,
			long lastCheckpointTs,
			long currentTs,
			FileSystem hadoopFs,
			Path cpParentDir,
			FileSystem localFs,
			Path localDir,
			Map<Interval, URI> intervalMapping) {

		super(backend, stateId, kvStateConf, keySerializer, namespaceSerializer, stateDesc, 0,
				lastCheckpointTs, currentTs);

		this.checkpointManager = new HdfsCheckpointManager(hadoopFs, cpParentDir, localFs, localDir, intervalMapping,
				kvStateConf);
	}

	@Override
	public KvStateSnapshot<K, N, ValueState<V>, ValueStateDescriptor<V>, FsStateBackend> snapshotStates(
			Collection<Entry<Tuple2<K, N>, Optional<V>>> modifiedKVs,
			long checkpointId,
			final long timestamp) throws IOException {

		checkpointManager.snapshotToLocal(modifiedKVs, timestamp, keySerializer, namespaceSerializer, valueSerializer);

		LookupFile mergedFile = checkpointManager.mergeLocalFilesToHdfs();

		KvStateSnapshot<K, N, ValueState<V>, ValueStateDescriptor<V>, FsStateBackend> snapshot = new HdfsKvStateSnapshot<K, N, V>(
				stateId, timestamp,
				mergedFile != null ? mergedFile.size() : 0,
				checkpointManager.getCheckpointDir(),
				checkpointManager.getLocalTmpDir(), checkpointManager.getMappingForSnapshot(), namespaceSerializer,
				stateDesc);

		return snapshot;
	}

	public void evictModified(Collection<Entry<Tuple2<K, N>, Optional<V>>> KVsToEvict, long lastCheckpointId,
			long lastCheckpointTs, long currentTs) throws IOException {

		checkpointManager.snapshotToLocal(KVsToEvict, currentTs, keySerializer, namespaceSerializer, valueSerializer);
	}

	@Override
	public Optional<V> lookupLatest(Tuple2<K, N> key, byte[] serializedKey) {
		try {
			return checkpointManager.lookupKey(serializedKey, valueSerializer);
		} catch (IOException e) {
			// We need to re-throw this exception to conform to the map
			// interface, we will catch this when we call the the put/get
			throw new RuntimeException("Could not get state for key: " + key, e);
		}
	}

	public HdfsCheckpointManager getCheckpointManager() {
		return checkpointManager;
	}

	@Override
	public void dispose() {
		try {
			checkpointManager.close();
		} catch (Exception e) {
		}
	}

	public static class HdfsKvStateSnapshot<K, N, V>
			implements KvStateSnapshot<K, N, ValueState<V>, ValueStateDescriptor<V>, FsStateBackend> {

		private static final long serialVersionUID = 1L;

		private long timestamp;
		private URI cpParentDir;
		private URI tmpDir;
		private Map<Interval, URI> intervalMapping;
		private String stateId;
		private long size;
		private TypeSerializer<N> namespaceSerializer;
		private ValueStateDescriptor<V> stateDesc;

		public HdfsKvStateSnapshot(String stateId, long timestamp, long size, Path cpParentDir, Path tmpDir,
				Map<Interval, URI> intervalMapping, TypeSerializer<N> namespaceSerializer,
				ValueStateDescriptor<V> stateDesc) {
			this.timestamp = timestamp;
			this.cpParentDir = cpParentDir.toUri();
			this.tmpDir = tmpDir.toUri();
			this.intervalMapping = intervalMapping;
			this.stateId = stateId;
			this.size = size;
			this.namespaceSerializer = namespaceSerializer;
			this.stateDesc = stateDesc;
		}

		@Override
		public KvState<K, N, ValueState<V>, ValueStateDescriptor<V>, FsStateBackend> restoreState(
				FsStateBackend stateBackend, TypeSerializer<K> keySerializer, ClassLoader classLoader,
				long recoveryTimestamp) throws Exception {

			HdfsStateBackend backend = (HdfsStateBackend) stateBackend;

			HdfsKvState<K, N, V> restored = new HdfsKvState<>(backend, stateId, backend.getKvStateConf(), keySerializer,
					namespaceSerializer, stateDesc, timestamp, recoveryTimestamp + 1, backend.getHadoopFileSystem(),
					new Path(cpParentDir), backend.getLocalFileSystem(), new Path(tmpDir),
					intervalMapping);

			LOG.debug("State {} has been successfully restored for checkoint ts: {}", stateId, timestamp);

			return restored;
		}

		@Override
		public void discardState() throws Exception {
			// Remove files?
		}

		@Override
		public long getStateSize() throws Exception {
			return size;
		}

	}

	@Override
	public void compact(final long from, final long to) throws IOException {
		LOG.debug("Starting compaction between {} and {}", from, to);
		executor.submit(new Callable<Void>() {

			@Override
			public Void call() throws Exception {
				checkpointManager.mergeCheckpoints(from, to);
				return null;
			}

		});
	}
}
