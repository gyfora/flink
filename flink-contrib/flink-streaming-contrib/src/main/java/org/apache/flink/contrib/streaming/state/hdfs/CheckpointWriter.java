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

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.contrib.streaming.state.KeyFunnel;
import org.apache.flink.util.InstantiationUtil;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.file.tfile.TFile.Writer;

import com.google.common.base.Optional;
import com.google.common.hash.BloomFilter;
import com.google.common.primitives.UnsignedBytes;

public class CheckpointWriter implements AutoCloseable {

	public static final int minBlockSize = 512;

	private FSDataOutputStream fout;
	private Writer writer;

	public CheckpointWriter(Path path, FileSystem fs) throws IOException {
		fout = fs.create(path);
		writer = new Writer(fout, minBlockSize, "none", "memcmp", fs.getConf());
	}

	public <V> void writeSorted(SortedMap<byte[], Optional<V>> kvPairs, TypeSerializer<V> valueSerializer)
			throws IOException {

		BloomFilter<byte[]> bloomFilter = BloomFilter.create(new KeyFunnel(), 1000000, 0.0001);

		for (Entry<byte[], Optional<V>> kv : kvPairs.entrySet()) {

			byte[] key = kv.getKey();
			Optional<V> valueOption = kv.getValue();

			bloomFilter.put(key);
			writer.append(key, valueOption.isPresent()
					? InstantiationUtil.serializeToByteArray(valueSerializer, valueOption.get())
					: new byte[0]);
		}

		DataOutputStream mo = writer.prepareMetaBlock("bloomfilter");
		bloomFilter.writeTo(mo);
		mo.close();
	}

	public <V> void writeUnsorted(Map<byte[], Optional<V>> kvPairs, TypeSerializer<V> valueSerializer)
			throws IOException {
		TreeMap<byte[], Optional<V>> sorted = new TreeMap<>(UnsignedBytes.lexicographicalComparator());
		sorted.putAll(kvPairs);
		writeSorted(sorted, valueSerializer);
	}

	@Override
	public void close() throws Exception {
		writer.close();
		fout.close();
	}

}
