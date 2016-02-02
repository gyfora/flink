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

package org.apache.flink.contrib.streaming.state.hdfs.tfile;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.KeyFunnel;
import org.apache.flink.contrib.streaming.state.hdfs.AbstractCheckpointWriter;
import org.apache.flink.util.InstantiationUtil;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.file.tfile.TFile.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.hash.BloomFilter;

public class TFileCheckpointWriter extends AbstractCheckpointWriter {

	private static final Logger LOG = LoggerFactory.getLogger(TFileCheckpointWriter.class);
	private static final long serialVersionUID = 1L;

	private final FileSystem fs;
	private final Path path;
	private final String compression;

	private FSDataOutputStream fout;
	private Writer writer;
	private BloomFilter<byte[]> bloomFilter;

	private int minBlockSize;
	private int estimatedKvSize;
	private final double bloomFilterFPP;
	private int bloomFilterSize;
	private int numKvPairs;
	private final int indexInterval;

	public TFileCheckpointWriter(Path path, FileSystem fs, int indexInterval, int maxBfSize, double bloomFilterFPP,
			String compressionString) throws IOException {
		this.fs = fs;
		this.path = path;
		this.compression = compressionString;
		this.bloomFilterFPP = bloomFilterFPP;
		this.indexInterval = indexInterval;
		bloomFilterSize = maxBfSize;
	}

	public void initWriter(int estimatedNumKvPairs, int kvPairSize) throws IOException {
		if (bloomFilter != null) {
			throw new RuntimeException("Cannot create bloomfilter twice.");
		}
		if (estimatedNumKvPairs < bloomFilterSize) {
			bloomFilterSize = estimatedNumKvPairs;
		}

		this.estimatedKvSize = kvPairSize;
		this.minBlockSize = (int) (kvPairSize
				* (indexInterval > 0 ? indexInterval : Math.max(1, Math.log(estimatedNumKvPairs))));

		this.fout = fs.create(path);
		this.writer = new Writer(fout, minBlockSize, compression, "memcmp", fs.getConf());
		this.bloomFilter = BloomFilter.create(new KeyFunnel(), estimatedNumKvPairs, bloomFilterFPP);

		LOG.debug("TFileCheckpointWriter initialized with bloomfilter size {}, min block size {} : {}", bloomFilterSize,
				minBlockSize, path);
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

		initWriter(kvPairs.size(), estimateRecordSize(kvPairs, valueSerializer, 10));

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

	private <V> int estimateRecordSize(SortedMap<byte[], Optional<V>> kvPairs, TypeSerializer<V> valueSerializer,
			int sampleSize) throws IOException {

		int totalSize = 0;
		int i = 0;
		Iterator<Entry<byte[], Optional<V>>> entryIt = kvPairs.entrySet().iterator();

		while (i++ < sampleSize && entryIt.hasNext()) {
			Entry<byte[], Optional<V>> e = entryIt.next();
			totalSize += e.getKey().length;
			totalSize += e.getValue().isPresent()
					? InstantiationUtil.serializeToByteArray(valueSerializer, e.getValue().get()).length : 0;
		}

		return totalSize / i;
	}

	public void append(byte[] key, byte[] value) throws IOException {
		bloomFilter.put(key);
		writer.append(key, value);
		numKvPairs++;
	}

	@Override
	public void close() throws Exception {
		if (bloomFilter != null) {
			DataOutputStream mo = writer.prepareMetaBlock("bloomfilter");
			mo.writeInt(numKvPairs);
			mo.writeInt(estimatedKvSize);
			bloomFilter.writeTo(mo);
			mo.close();
		}
		if (writer != null) {
			writer.close();
		}
		if (fout != null) {
			fout.close();
		}
	}

}
