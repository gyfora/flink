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

import org.apache.flink.contrib.streaming.state.hdfs.CheckpointReader;
import org.apache.flink.contrib.streaming.state.hdfs.mapfile.BloomMapFile.Reader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BloomMapFileCheckpointReader implements CheckpointReader {

	private static final Logger LOG = LoggerFactory.getLogger(BloomMapFileCheckpointReader.class);
	private static final long serialVersionUID = 1L;

	private Reader reader;

	public BloomMapFileCheckpointReader(Path path, Configuration conf) throws IOException {
		LOG.debug("Creating BloomMapFileCheckpointReader for {} with conf: {}", path, conf);
		reader = new Reader(path, conf);
	}

	public Reader getReader() {
		return reader;
	}

	public synchronized byte[] lookup(byte[] key) throws IOException {
		BytesWritable val = new BytesWritable();
		BytesWritable read = (BytesWritable) reader.get(new BytesWritable(key), val);
		return read != null ? read.getBytes() : null;
	}

	@Override
	public synchronized void close() throws Exception {
		if (reader != null) {
			reader.close();
		}
	}

}
