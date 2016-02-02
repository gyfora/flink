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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.flink.contrib.streaming.state.hdfs.CheckpointMerger;
import org.apache.flink.contrib.streaming.state.hdfs.Interval;
import org.apache.flink.contrib.streaming.state.hdfs.LookupFile;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.file.tfile.TFile.Reader;
import org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner;
import org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner.Entry;

import com.google.common.primitives.UnsignedBytes;

public class TFileMerger implements CheckpointMerger {

	private Comparator<byte[]> comparator = UnsignedBytes.lexicographicalComparator();
	private Reader[] inReaders;
	private Scanner[] scanners;
	private Interval[] intervals;
	private TFileCheckpointWriter outWriter;

	public TFileMerger(List<LookupFile> inFiles, LookupFile outFile, int indexInterval, int maxBfSize, double bfFPP,
			String compression,
			double mergeOverlap) throws IOException {
		inReaders = new Reader[inFiles.size()];
		scanners = new Scanner[inFiles.size()];
		intervals = new Interval[inFiles.size()];
		int[] numKeys = new int[inFiles.size()];

		long weightedSumKvPairSizes = 0;

		for (int i = 0; i < inFiles.size(); i++) {
			LookupFile lf = inFiles.get(i);
			Reader r = ((TFileCheckpointReader) lf.createNewReader()).getReader();
			inReaders[i] = r;
			scanners[i] = r.createScanner();
			intervals[i] = lf.interval;
			DataInputStream bi = r.getMetaBlock("bloomfilter");
			numKeys[i] = bi.readInt();
			weightedSumKvPairSizes += numKeys[i] * bi.readInt();
			bi.close();
		}

		Arrays.sort(numKeys);
		int mergeNumKeys = numKeys[inFiles.size() - 1];
		int totalNumKeys = numKeys[inFiles.size() - 1];

		for (int i = inFiles.size() - 2; i >= 0; i--) {
			mergeNumKeys += (1 - mergeOverlap) * numKeys[i];
			totalNumKeys += numKeys[i];
		}

		outWriter = new TFileCheckpointWriter(outFile.path, outFile.fs, indexInterval, maxBfSize, bfFPP, compression);
		outWriter.initWriter(mergeNumKeys, (int) (weightedSumKvPairSizes / totalNumKeys));
	}

	private void readNext(int index, byte[][] keys, byte[][] values) throws IOException {
		Scanner s = scanners[index];
		if (!s.atEnd()) {
			Entry entry = scanners[index].entry();
			keys[index] = new byte[entry.getKeyLength()];
			values[index] = new byte[entry.getValueLength()];
			entry.getKey(keys[index]);
			entry.getValue(values[index]);
			s.advance();
		} else {
			keys[index] = null;
			values[index] = null;
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
		byte[][] keys = new byte[inReaders.length][];
		byte[][] values = new byte[inReaders.length][];
		// Read first key/value from all inputs
		for (int i = 0; i < inReaders.length; i++) {
			readNext(i, keys, values);
		}

		do {
			int currentEntry = -1;
			byte[] currentKey = null;
			byte[] currentValue = null;
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
			readNext(currentEntry, keys, values);
		} while (true);
	}

	public void close() throws Exception {

		if (outWriter != null) {
			outWriter.close();
			outWriter = null;
		}

		for (int i = 0; i < inReaders.length; i++) {
			IOUtils.closeStream(inReaders[i]);
			IOUtils.closeStream(scanners[i]);
			inReaders[i] = null;
			scanners[i] = null;
		}
	}
}
