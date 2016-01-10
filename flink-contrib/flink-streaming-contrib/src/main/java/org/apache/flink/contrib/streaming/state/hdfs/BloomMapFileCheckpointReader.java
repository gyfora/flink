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
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapFile;

public class BloomMapFileCheckpointReader implements CheckpointReader {

	private static final long serialVersionUID = 1L;

	private MapFile.Reader reader;

	public BloomMapFileCheckpointReader(Path path) throws IOException {
		reader = createReader(path);
	}

	public MapFile.Reader createReader(Path path) throws IOException {
		return new Reader(path, new Configuration());
	}

	public byte[] lookup(byte[] key) throws IOException {
		BytesWritable val = new BytesWritable();
		reader.get(new BytesWritable(key), val);
		return val.getBytes();
	}

	@Override
	public void close() throws Exception {
		reader.close();
	}

}
