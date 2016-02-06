/**
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

package org.apache.flink.contrib.streaming.state.hdfs.mapfile;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.bloom.DynamicBloomFilter;
import org.apache.hadoop.util.bloom.Filter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class extends {@link MapFile} and provides very much the same
 * functionality. However, it uses dynamic Bloom filters to provide quick
 * membership test for keys, and it offers a fast version of
 * {@link Reader#get(WritableComparable, Writable)} operation, especially in
 * case of sparsely populated MapFile-s.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class BloomMapFile {
	private static final Logger LOG = LoggerFactory.getLogger(BloomMapFile.class);
	public static final String BLOOM_FILE_NAME = "bloom";
	public static final int HASH_COUNT = 5;

	public static void delete(FileSystem fs, String name) throws IOException {
		Path dir = new Path(name);
		Path data = new Path(dir, MapFile.DATA_FILE_NAME);
		Path index = new Path(dir, MapFile.INDEX_FILE_NAME);
		Path bloom = new Path(dir, BLOOM_FILE_NAME);

		fs.delete(data, true);
		fs.delete(index, true);
		fs.delete(bloom, true);
		fs.delete(dir, true);
	}

	private static byte[] byteArrayForBloomKey(DataOutputBuffer buf) {
		int cleanLength = buf.getLength();
		byte[] ba = buf.getData();
		if (cleanLength != ba.length) {
			ba = new byte[cleanLength];
			System.arraycopy(buf.getData(), 0, ba, 0, cleanLength);
		}
		return ba;
	}

	public static class Writer extends MapFile.Writer {
		private DynamicBloomFilter bloomFilter;
		private Key bloomKey = new Key();
		private DataOutputBuffer buf = new DataOutputBuffer();
		private FileSystem fs;
		private Path dir;
		private int numKvsWritten = 0;

		public Writer(Configuration conf, int numKeys, double bfFPP, Path dir,
				SequenceFile.Writer.Option... options) throws IOException {
			super(conf, dir, options);
			this.fs = dir.getFileSystem(conf);
			this.dir = dir;
			initBloomFilter(conf, numKeys, bfFPP);
		}

		private synchronized void initBloomFilter(Configuration conf, int numKeys, double bfFPP) {
			float errorRate = (float) bfFPP;
			int vectorSize = (int) Math.ceil((double) (-HASH_COUNT * numKeys) /
					Math.log(1.0 - Math.pow(errorRate, 1.0 / HASH_COUNT)));
			bloomFilter = new DynamicBloomFilter(vectorSize, HASH_COUNT,
					Hash.getHashType(conf), numKeys);
		}

		@Override
		public synchronized void append(@SuppressWarnings("rawtypes") WritableComparable key, Writable val)
				throws IOException {
			super.append(key, val);
			buf.reset();
			key.write(buf);
			bloomKey.set(byteArrayForBloomKey(buf), 1.0);
			bloomFilter.add(bloomKey);
			numKvsWritten++;
		}

		@Override
		public synchronized void close() throws IOException {
			super.close();
			DataOutputStream out = fs.create(new Path(dir, BLOOM_FILE_NAME), true);
			try {
				out.writeInt(numKvsWritten);
				bloomFilter.write(out);
				out.flush();
				out.close();
				out = null;
			} finally {
				IOUtils.closeStream(out);
			}
		}

	}

	public static class Reader extends MapFile.Reader {
		private DynamicBloomFilter bloomFilter;
		private DataOutputBuffer buf = new DataOutputBuffer();
		private Key bloomKey = new Key();
		private int numKeys;

		public Reader(Path dir, Configuration conf,
				SequenceFile.Reader.Option... options) throws IOException {
			super(dir, conf, options);
			initBloomFilter(dir, conf);
		}

		public int getNumKeys() {
			return numKeys;
		}

		private void initBloomFilter(Path dirName,
				Configuration conf) {

			DataInputStream in = null;
			try {
				FileSystem fs = dirName.getFileSystem(conf);
				in = fs.open(new Path(dirName, BLOOM_FILE_NAME));
				numKeys = in.readInt();
				bloomFilter = new DynamicBloomFilter();
				bloomFilter.readFields(in);
				in.close();
				in = null;
			} catch (IOException ioe) {
				LOG.warn("Can't open BloomFilter: " + ioe + " - fallback to MapFile.");
				bloomFilter = null;
			} finally {
				IOUtils.closeStream(in);
			}
		}

		/**
		 * Checks if this MapFile has the indicated key. The membership test is
		 * performed using a Bloom filter, so the result has always non-zero
		 * probability of false positives.
		 * 
		 * @param key
		 *            key to check
		 * @return false iff key doesn't exist, true if key probably exists.
		 * @throws IOException
		 */
		public boolean probablyHasKey(@SuppressWarnings("rawtypes") WritableComparable key) throws IOException {
			if (bloomFilter == null) {
				return true;
			}
			buf.reset();
			key.write(buf);
			bloomKey.set(byteArrayForBloomKey(buf), 1.0);
			return bloomFilter.membershipTest(bloomKey);
		}

		/**
		 * Fast version of the
		 * {@link MapFile.Reader#get(WritableComparable, Writable)} method.
		 * First it checks the Bloom filter for the existence of the key, and
		 * only if present it performs the real get operation. This yields
		 * significant performance improvements for get operations on sparsely
		 * populated files.
		 */
		@Override
		public synchronized Writable get(@SuppressWarnings("rawtypes") WritableComparable key, Writable val)
				throws IOException {
			if (!probablyHasKey(key)) {
				return null;
			}
			Writable out = super.get(key, val);
			return out;
		}

		/**
		 * Retrieve the Bloom filter used by this instance of the Reader.
		 * 
		 * @return a Bloom filter (see {@link Filter})
		 */
		public Filter getBloomFilter() {
			return bloomFilter;
		}
	}
}
