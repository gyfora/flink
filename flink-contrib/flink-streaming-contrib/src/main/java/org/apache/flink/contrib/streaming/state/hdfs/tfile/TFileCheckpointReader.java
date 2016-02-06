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

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.flink.contrib.streaming.state.KeyFunnel;
import org.apache.flink.contrib.streaming.state.hdfs.CheckpointReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.file.tfile.TFile.Reader;
import org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner;

import com.google.common.hash.BloomFilter;

public class TFileCheckpointReader implements CheckpointReader {

	private static final long serialVersionUID = 1L;

	private final Reader reader;
	private final Scanner scanner;
	private final BloomFilter<byte[]> bloomfilter;
	private final int bfSize;

	public TFileCheckpointReader(Path path, FileSystem fs) throws IOException {
		reader = new Reader(fs.open(path), fs.getFileStatus(path).getLen(), fs.getConf());
		scanner = reader.createScanner();

		DataInputStream bi = reader.getMetaBlock("bloomfilter");
		bfSize = bi.readInt();
		bi.readInt();
		bloomfilter = BloomFilter.readFrom(bi, new KeyFunnel());
		bi.close();
	}

	public Reader getReader() {
		return reader;
	}

	public Scanner getScanner() {
		return scanner;
	}

	public synchronized byte[] lookup(byte[] key) throws IOException {

		boolean bfMightContain = bloomfilter.mightContain(key);

		if (bfMightContain) {
			if (scanner.seekTo(key)) {
				int valueLen = scanner.entry().getValueLength();
				byte[] read = new byte[valueLen];
				scanner.entry().getValue(read);
				return read;
			}
		}

		return null;
	}

	public int getBloomFilterSize() {
		return bfSize;
	}

	@Override
	public synchronized void close() throws Exception {
		if (scanner != null) {
			scanner.close();
		}
		if (reader != null) {
			reader.close();
		}
	}

}
