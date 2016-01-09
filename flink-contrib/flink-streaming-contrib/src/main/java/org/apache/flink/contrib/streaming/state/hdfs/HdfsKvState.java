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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.OutOfCoreKvState;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.util.InstantiationUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;

public class HdfsKvState<K, V> extends OutOfCoreKvState<K, V, FsStateBackend> {

	private static final Logger LOG = LoggerFactory.getLogger(HdfsKvState.class);

	private final CheckpointerFactory cpFactory;

	private final Path cpParentDir;
	private final KeyScanner scanner;

	private final FileSystem fs;

	public HdfsKvState(
			HdfsKvStateConfig kvStateConf,
			TypeSerializer<K> keySerializer,
			TypeSerializer<V> valueSerializer,
			V defaultValue,
			long lastCheckpointTs,
			long currentTs,
			FileSystem fs,
			Path cpParentDir,
			List<Path> cpFiles) {

		super(kvStateConf, keySerializer, valueSerializer, defaultValue, 0,
				lastCheckpointTs, currentTs);

		this.cpFactory = kvStateConf.getCheckpointerFactory();

		this.cpParentDir = cpParentDir;

		// We make sure our lookup file a sorted in descending order by
		// name (timestamp)
		List<Path> sortedCpFiles = new ArrayList<>(cpFiles);
		Collections.sort(cpFiles, new Comparator<Path>() {
			@Override
			public int compare(Path o1, Path o2) {
				return -1 * Long.compare(Long.parseLong(o1.getName()), Long.parseLong(o1.getName()));
			}
		});

		this.scanner = new KeyScanner(fs, sortedCpFiles, kvStateConf);
		this.fs = fs;
	}

	@Override
	public void snapshotModified(Collection<Entry<K, Optional<V>>> modifiedKVs, long checkpointId, long timestamp) {
		if (!modifiedKVs.isEmpty()) {
			// We use the timestamp as filename (this is assumed to be
			// increasing between snapshots)
			Path cpFile = new Path(cpParentDir, String.valueOf(timestamp));

			// Write the sorted k-v pairs to the new file
			try (CheckpointWriter writer = cpFactory.createWriter(fs, cpFile, conf)) {
				Tuple2<Double, Double> kbytesWritten = writer.writeUnsorted(modifiedKVs, keySerializer,
						valueSerializer);
				LOG.debug("Successfully written checkpoint to disk - (keyKBytes, valueKBytes) = {}", kbytesWritten);
			} catch (Exception e) {
				throw new RuntimeException("Could not write checkpoint to disk.", e);
			}

			// Add the new checkpoint file to the scanner for future lookups
			scanner.addNewLookupFile(cpFile);
		}
	}

	@Override
	public Optional<V> lookupLatest(K key) {
		try {
			final byte[] serializedKey = InstantiationUtil.serializeToByteArray(keySerializer, key);

			return scanner.lookup(serializedKey, valueSerializer);
		} catch (IOException e) {
			// We need to re-throw this exception to conform to the map
			// interface, we will catch this when we call the the put/get
			throw new RuntimeException("Could not get state for key: " + key, e);
		}
	}

	@Override
	public KvStateSnapshot<K, V, FsStateBackend> createSnapshot(long checkpointId, long timestamp) {
		return new HdfsKvStateSnapshot<K, V>(timestamp, cpParentDir, scanner.getPaths());
	}

	public KeyScanner getKeyScanner() {
		return scanner;
	}

	@Override
	public int size() {
		return 0;
	}

	@Override
	public void dispose() {
		// Remove all files?
	}

	private static class HdfsKvStateSnapshot<K, V> implements KvStateSnapshot<K, V, FsStateBackend> {

		private static final long serialVersionUID = 1L;

		private long timestamp;
		private URI cpParentDir;
		private List<URI> paths;

		public HdfsKvStateSnapshot(long timestamp, Path cpParentDir, List<Path> paths) {
			this.timestamp = timestamp;
			this.cpParentDir = cpParentDir.toUri();
			this.paths = new ArrayList<>(Lists.transform(paths, new Function<Path, URI>() {

				@Override
				public URI apply(Path path) {
					return path.toUri();
				}
			}));
		}

		@Override
		public KvState<K, V, FsStateBackend> restoreState(FsStateBackend stateBackend, TypeSerializer<K> keySerializer,
				TypeSerializer<V> valueSerializer, V defaultValue, ClassLoader classLoader, long recoveryTimestamp)
						throws Exception {

			HdfsStateBackend backend = (HdfsStateBackend) stateBackend;

			return new HdfsKvState<>(backend.getKvStateConf(), keySerializer, valueSerializer,
					defaultValue, timestamp, timestamp + 1, backend.getHadoopFileSystem(), new Path(cpParentDir),
					Lists.transform(paths, new Function<URI, Path>() {

						@Override
						public Path apply(URI uri) {
							return new Path(uri);
						}
					}));
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
