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
import java.util.Map.Entry;
import java.util.SortedMap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.KeyFunnel;
import org.apache.flink.util.InstantiationUtil;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.file.tfile.TFile.Writer;

import com.google.common.base.Optional;
import com.google.common.hash.BloomFilter;

public class TFileCheckpointWriter extends AbstractCheckpointWriter {

	private static final long serialVersionUID = 1L;

	public static final int minBlockSize = 512;

	private final FSDataOutputStream fout;
	private final Writer writer;
	private final BloomFilter<byte[]> bloomFilter;

	public TFileCheckpointWriter(Path path, FileSystem fs) throws IOException {
		this(path, fs, 1000000, 0.0001);
	}

	public TFileCheckpointWriter(Path path, FileSystem fs, int bloomFilterExpectedInserts, double bloomFilterFPP)
			throws IOException {
		fout = fs.create(path);
		writer = new Writer(fout, minBlockSize, "none", "memcmp", fs.getConf());
		this.bloomFilter = BloomFilter.create(new KeyFunnel(), bloomFilterExpectedInserts,
				bloomFilterFPP);
	}

	/**
	 * Writes a {@link SortedMap} of byte[]-Optional<V> pairs to disk using the
	 * given {@link TypeSerializer} to serialize the values. Returns the total
	 * number of key and value Kbytes written.
	 * 
	 * @param kvPairs
	 *            K-Optional<V> pairs to write
	 * @param valueSerializer
	 *            {@link TypeSerializer} for the values
	 * @return Tuple2(totalKeyKBytes, totalValueKBytes)
	 */
	public <V> Tuple2<Double, Double> writeSorted(SortedMap<byte[], Optional<V>> kvPairs,
			TypeSerializer<V> valueSerializer)
					throws IOException {

		long totalKeyBytes = 0;
		long totalValueBytes = 0;

		for (Entry<byte[], Optional<V>> kv : kvPairs.entrySet()) {

			byte[] key = kv.getKey();
			Optional<V> valueOption = kv.getValue();
			byte[] valueBytes = valueOption.isPresent()
					? InstantiationUtil.serializeToByteArray(valueSerializer, valueOption.get())
					: new byte[0];

			append(key, valueBytes);

			totalKeyBytes += key.length;
			totalValueBytes += valueBytes.length;
		}

		return Tuple2.of(totalKeyBytes / 1024., totalValueBytes / 1024.);
	}

	public void append(byte[] key, byte[] value) throws IOException {
		bloomFilter.put(key);
		writer.append(key, value);
	}

	@Override
	public void close() throws Exception {
		DataOutputStream mo = writer.prepareMetaBlock("bloomfilter");
		bloomFilter.writeTo(mo);
		mo.close();
		writer.close();
		fout.close();
	}

}
