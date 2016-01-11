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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BloomMapFile.Reader;
import org.apache.hadoop.io.BloomMapFile.Writer;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.WritableComparator;

public class BloomMapFileMerger implements AutoCloseable {
	private WritableComparator comparator = new BytesWritable.Comparator();
	private Reader[] inReaders;
	private Writer outWriter;

	public BloomMapFileMerger(Configuration conf, Path[] inMapFiles, Path outMapFile) throws IOException {
		inReaders = new Reader[inMapFiles.length];
		for (int i = 0; i < inMapFiles.length; i++) {
			Reader reader = new Reader(inMapFiles[i], conf);
			inReaders[i] = reader;
		}

		outWriter = new Writer(conf, outMapFile,
				Writer.keyClass(BytesWritable.class),
				Writer.valueClass(BytesWritable.class));
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
	public void mergePass() throws IOException {
		// re-usable array
		BytesWritable[] keys = new BytesWritable[inReaders.length];
		BytesWritable[] values = new BytesWritable[inReaders.length];
		// Read first key/value from all inputs
		for (int i = 0; i < inReaders.length; i++) {
			keys[i] = new BytesWritable();
			values[i] = new BytesWritable();
			if (!inReaders[i].next(keys[i], values[i])) {
				// Handle empty files
				keys[i] = null;
				values[i] = null;
			}
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
			if (!inReaders[currentEntry].next(keys[currentEntry],
					values[currentEntry])) {
				// EOF for this file
				keys[currentEntry] = null;
				values[currentEntry] = null;
			}
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
