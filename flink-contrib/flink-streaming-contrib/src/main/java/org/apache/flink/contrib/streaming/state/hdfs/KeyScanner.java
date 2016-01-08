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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.InstantiationUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;

public class KeyScanner implements AutoCloseable {
	private final LinkedList<Path> paths;
	private final Map<Path, CheckpointReader> openReaders = new HashMap<>();
	private final FileSystem fs;
	private final HdfsKvStateConfig conf;

	public KeyScanner(FileSystem fs, List<Path> paths, HdfsKvStateConfig conf) {
		this.paths = new LinkedList<>(paths);
		this.fs = fs;
		this.conf = conf;
	}

	public List<Path> getPaths() {
		return paths;
	}

	public void addNewLookupFile(Path path) {
		paths.addFirst(path);
	}

	public <V> Optional<V> lookup(byte[] key, TypeSerializer<V> valueSerializer) throws IOException {
		for (Path checkpointPath : paths) {
			CheckpointReader reader = openReaders.get(checkpointPath);
			if (reader == null) {
				reader = conf.getCheckpointerFactory().createReader(fs, checkpointPath, conf);
				openReaders.put(checkpointPath, reader);
			}
			byte[] val = reader.lookup(key);
			if (val != null) {
				return val.length > 0
						? Optional.of(InstantiationUtil.deserializeFromByteArray(valueSerializer, val))
						: Optional.<V> absent();
			}
		}
		return Optional.absent();
	}

	@Override
	public void close() throws Exception {
		for (CheckpointReader r : openReaders.values()) {
			r.close();
		}
	}

	@Override
	public String toString() {
		return "KeyScanner: " + paths;
	}

}
