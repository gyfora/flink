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

import java.io.IOException;
import java.util.List;

import org.apache.flink.contrib.streaming.state.KvStateConfig;
import org.apache.flink.contrib.streaming.state.hdfs.CheckpointMerger;
import org.apache.flink.contrib.streaming.state.hdfs.CheckpointReader;
import org.apache.flink.contrib.streaming.state.hdfs.CheckpointWriter;
import org.apache.flink.contrib.streaming.state.hdfs.CheckpointerFactory;
import org.apache.flink.contrib.streaming.state.hdfs.LookupFile;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TFileCheckpointerFactory implements CheckpointerFactory {

	private static final long serialVersionUID = 1L;
	private int maxBfSize = 10000000;
	private double bfFPP = 0.0001;
	private double mergeOverlap = 0.5;
	private String compressionString = "none";
	private int indexInterval = 128;

	@Override
	public CheckpointReader createReader(FileSystem fs, Path path, KvStateConfig<?> conf) throws IOException {
		return new TFileCheckpointReader(path, fs);
	}

	@Override
	public CheckpointWriter createWriter(FileSystem fs, Path path, KvStateConfig<?> conf) throws IOException {
		return new TFileCheckpointWriter(path, fs, indexInterval, maxBfSize, bfFPP, compressionString);
	}

	@Override
	public CheckpointMerger createMerger(List<LookupFile> inFiles, LookupFile outFile, KvStateConfig<?> conf)
			throws IOException {
		return new TFileMerger(inFiles, outFile, indexInterval, maxBfSize, bfFPP, compressionString, mergeOverlap);
	}

	public TFileCheckpointerFactory setBFParams(int maxBfSize, double bfFPP) {
		this.maxBfSize = maxBfSize;
		this.bfFPP = bfFPP;
		return this;
	}

	public TFileCheckpointerFactory setMergeOverlap(double mergeOverlap) {
		this.mergeOverlap = mergeOverlap;
		return this;
	}

	public TFileCheckpointerFactory setCompression(String compressionString) {
		this.compressionString = compressionString;
		return this;
	}

	public TFileCheckpointerFactory setIndexInterval(int indexInterval) {
		this.indexInterval = indexInterval;
		return this;
	}

}
