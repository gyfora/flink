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

package org.apache.flink.contrib.streaming.state.hdfs.mapfile;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.contrib.streaming.state.hdfs.CheckpointMerger;
import org.apache.flink.contrib.streaming.state.hdfs.Interval;
import org.apache.flink.contrib.streaming.state.hdfs.LookupFile;
import org.apache.flink.contrib.streaming.state.hdfs.mapfile.BloomMapFile.Reader;
import org.apache.flink.contrib.streaming.state.hdfs.mapfile.BloomMapFile.Writer;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.WritableComparator;

public class BloomMapFileMerger implements CheckpointMerger {
	private WritableComparator comparator = new BytesWritable.Comparator();
	private Reader[] inReaders;
	private Interval[] intervals;
	private Writer outWriter;

	@SuppressWarnings("resource")
	public BloomMapFileMerger(List<LookupFile> inFiles, LookupFile outFile, int bfMaxSize,
			int indexInterval,
			double mergeOverlap)
					throws IOException {
		inReaders = new Reader[inFiles.size()];
		intervals = new Interval[inFiles.size()];
		int[] numKeys = new int[inFiles.size()];

		for (int i = 0; i < inFiles.size(); i++) {
			LookupFile lf = inFiles.get(i);
			inReaders[i] = ((BloomMapFileCheckpointReader) lf.createNewReader()).getReader();
			intervals[i] = lf.interval;
			numKeys[i] = inReaders[i].getNumKeys();
		}

		Arrays.sort(numKeys);
		int mergeNumKeys = numKeys[inFiles.size() - 1];

		for (int i = inFiles.size() - 2; i >= 0; i--) {
			mergeNumKeys += (1 - mergeOverlap) * numKeys[i];
		}

		outWriter = new BloomMapFileCheckpointWriter(outFile.path, outFile.fs.getConf(), bfMaxSize, indexInterval)
				.createWriter(mergeNumKeys);
	}

	private void readNext(int i, BytesWritable[] keys, BytesWritable[] values) throws IOException {
		if (!inReaders[i].next(keys[i], values[i])) {
			keys[i] = null;
			values[i] = null;
		}
	}

	/**
	 * Merge all input files to output map file.<br>
	 * 1. Read first key/value from all input files to keys/values array. <br>
	 * 2. Select the least key and corresponding value. <br>
	 * 3. Write the selected key and value to output file. <br>
	 * 4. Replace the already written key/value in keys/values arrays with the
	 * next key/value from the selected input <br>
	 * 5. Repeat step 2-4 till all keys are read. <br>
	 */
	public void merge() throws IOException {
		// re-usable array
		BytesWritable[] keys = new BytesWritable[inReaders.length];
		BytesWritable[] values = new BytesWritable[inReaders.length];
		// Read first key/value from all inputs
		for (int i = 0; i < inReaders.length; i++) {
			keys[i] = new BytesWritable();
			values[i] = new BytesWritable();
			readNext(i, keys, values);
		}

		do {
			int currentEntry = -1;
			BytesWritable currentKey = null;
			BytesWritable currentValue = null;
			for (int i = 0; i < keys.length; i++) {
				if (keys[i] == null) {
					// Skip Readers reached EOF
					continue;
				}
				if (currentKey == null || comparator.compare(currentKey, keys[i]) > 0) {
					currentEntry = i;
					currentKey = keys[i];
					currentValue = values[i];
				} else if (currentKey != null && comparator.compare(currentKey, keys[i]) == 0) {
					// If equal keep latest, drop oldest
					if (intervals[i].isAfter(intervals[currentEntry])) {
						readNext(currentEntry, keys, values);
						currentEntry = i;
						currentKey = keys[i];
						currentValue = values[i];
					} else {
						readNext(i, keys, values);
					}
				}
			}
			if (currentKey == null) {
				// Merge Complete
				break;
			}
			// Write the selected key/value to merge stream
			outWriter.append(currentKey, currentValue);
			// Replace the already written key/value in keys/values arrays with
			// the next key/value from the selected input
			readNext(currentEntry, keys, values);
		} while (true);
	}

	public void close() throws IOException {
		for (int i = 0; i < inReaders.length; i++) {
			IOUtils.closeStream(inReaders[i]);
			inReaders[i] = null;
		}
		if (outWriter != null) {
			outWriter.close();
			outWriter = null;
		}
	}
}
