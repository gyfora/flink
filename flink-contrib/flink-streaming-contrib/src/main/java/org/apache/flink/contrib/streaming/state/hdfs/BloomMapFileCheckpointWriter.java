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
import java.util.Map.Entry;
import java.util.SortedMap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.InstantiationUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BloomMapFile.Writer;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapFile;

import com.google.common.base.Optional;

public class BloomMapFileCheckpointWriter extends AbstractCheckpointWriter {

	private static final long serialVersionUID = 1L;

	private final MapFile.Writer writer;

	public BloomMapFileCheckpointWriter(Path path)
			throws IOException {

		writer = createWriter(path);
	}

	public MapFile.Writer createWriter(Path path) throws IOException {
		return new Writer(new Configuration(), path, Writer.keyClass(BytesWritable.class),
				Writer.valueClass(BytesWritable.class));
	}

	public <V> Tuple2<Double, Double> writeSorted(SortedMap<byte[], Optional<V>> kvPairs,
			TypeSerializer<V> valueSerializer)
					throws IOException {
		long totalKeyBytes = 0;
		long totalValueBytes = 0;

		for (Entry<byte[], Optional<V>> kv : kvPairs.entrySet()) {
			byte[] key = kv.getKey();
			byte[] value = kv.getValue().isPresent()
					? InstantiationUtil.serializeToByteArray(valueSerializer, kv.getValue().get())
					: new byte[0];
			writer.append(new BytesWritable(key), new BytesWritable(value));

			totalKeyBytes += key.length;
			totalValueBytes += value.length;
		}

		return Tuple2.of(totalKeyBytes / 1024., totalValueBytes / 1024.);
	}

	@Override
	public void close() throws Exception {
		writer.close();
	}

}
