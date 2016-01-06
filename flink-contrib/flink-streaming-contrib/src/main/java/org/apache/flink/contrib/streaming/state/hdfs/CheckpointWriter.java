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
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.file.tfile.TFile.Writer;

import com.google.common.primitives.UnsignedBytes;

public class CheckpointWriter implements AutoCloseable {

	public static final int minBlockSize = 512;

	private FSDataOutputStream fout;
	private Writer writer;

	public CheckpointWriter(Path path, FileSystem fs) throws IOException {
		fout = fs.create(path);
		writer = new Writer(fout, minBlockSize, "none", "memcmp", fs.getConf());
	}

	public void writeSorted(SortedMap<byte[], byte[]> kvPairs) throws IOException {
		for (Entry<byte[], byte[]> kv : kvPairs.entrySet()) {
			writer.append(kv.getKey(), kv.getValue());
		}
	}

	public void writeUnsorted(Map<byte[], byte[]> kvPairs) throws IOException {
		TreeMap<byte[], byte[]> sorted = new TreeMap<>(UnsignedBytes.lexicographicalComparator());
		sorted.putAll(kvPairs);
		writeSorted(sorted);
	}

	@Override
	public void close() throws Exception {
		writer.close();
		fout.close();
	}

}
