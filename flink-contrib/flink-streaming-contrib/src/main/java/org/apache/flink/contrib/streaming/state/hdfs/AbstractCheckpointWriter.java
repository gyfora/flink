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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import com.google.common.base.Optional;
import com.google.common.primitives.UnsignedBytes;

public abstract class AbstractCheckpointWriter implements CheckpointWriter {

	private static final long serialVersionUID = 1L;

	@Override
	public <K, N, V> Tuple2<Double, Double> writeUnsorted(Collection<Entry<Tuple2<K, N>, Optional<V>>> kvPairs,
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			TypeSerializer<V> valueSerializer) throws IOException {

		SortedMap<byte[], Optional<V>> sortedKVs = new TreeMap<>(UnsignedBytes.lexicographicalComparator());

		for (Entry<Tuple2<K, N>, Optional<V>> entry : kvPairs) {
			sortedKVs.put(serialize(entry.getKey(), keySerializer, namespaceSerializer),
					entry.getValue());
		}

		return writeSorted(sortedKVs, valueSerializer);
	}

	public static <K, N, V> byte[] serialize(Tuple2<K, N> key, TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(baos);
		keySerializer.serialize(key.f0, out);
		namespaceSerializer.serialize(key.f1, out);
		out.close();
		return baos.toByteArray();
	}
}
