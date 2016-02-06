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
import org.apache.flink.contrib.streaming.state.logging.BranchLogger;
import org.apache.flink.contrib.streaming.state.logging.PerformanceLogger;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.file.tfile.TFile.Reader;
import org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.BloomFilter;

public class TFileCheckpointReader implements CheckpointReader {

	private static final Logger LOG = LoggerFactory.getLogger(TFileCheckpointReader.class);

	private static final long serialVersionUID = 1L;

	private final Reader reader;
	private final Scanner scanner;
	private final BloomFilter<byte[]> bloomfilter;
	private final int bfSize;
	private final PerformanceLogger readLogger;
	private final PerformanceLogger scanLogger;
	private final BranchLogger bfHitLogger;

	public TFileCheckpointReader(Path path, FileSystem fs) throws IOException {
		reader = new Reader(fs.open(path), fs.getFileStatus(path).getLen(), fs.getConf());
		scanner = reader.createScanner();

		DataInputStream bi = reader.getMetaBlock("bloomfilter");
		bfSize = bi.readInt();
		bi.readInt();
		bloomfilter = BloomFilter.readFrom(bi, new KeyFunnel());
		bi.close();
		readLogger = new PerformanceLogger(10000, "TFile read: " + path.toString(), LOG);
		scanLogger = new PerformanceLogger(10000, "TFile scan: " + path.toString(), LOG);
		bfHitLogger = new BranchLogger(50000, "TFile bf hit: " + path.toString(), LOG);
	}

	public Reader getReader() {
		return reader;
	}

	public Scanner getScanner() {
		return scanner;
	}

	public synchronized byte[] lookup(byte[] key) throws IOException {

		bfHitLogger.start();
		boolean bfMightContain = bloomfilter.mightContain(key);

		if (bfMightContain) {
			bfHitLogger.hit();
			scanLogger.startMeasurement();
			boolean found = scanner.seekTo(key);
			scanLogger.stopMeasurement();
			if (found) {
				readLogger.startMeasurement();
				int valueLen = scanner.entry().getValueLength();
				byte[] read = new byte[valueLen];
				scanner.entry().getValue(read);
				readLogger.stopMeasurement();
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
