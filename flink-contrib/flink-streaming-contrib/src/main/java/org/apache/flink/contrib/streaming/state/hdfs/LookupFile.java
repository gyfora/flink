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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LookupFile implements AutoCloseable {

	private static final Logger LOG = LoggerFactory.getLogger(LookupFile.class);

	public final Path path;
	public final FileSystem fs;
	public final Interval interval;
	public final boolean isLocal;

	private final HdfsKvStateConfig conf;
	private final CheckpointerFactory cf;
	private CheckpointReader reader;
	private boolean deleted = false;

	public LookupFile(Path path, FileSystem fs, boolean isLocal, HdfsKvStateConfig conf) {
		this(path, fs, Interval.pathToInterval(path), isLocal, conf);
	}

	public LookupFile(Path path, FileSystem fs, Interval interval, boolean isLocal, HdfsKvStateConfig conf) {
		this.path = path;
		this.fs = fs;
		this.interval = interval;
		this.conf = conf;
		this.cf = conf.getCheckpointerFactory();
		this.isLocal = isLocal;
	}

	public synchronized CheckpointReader getReader() throws IOException {
		if (reader != null) {
			return reader;
		} else if (deleted || !exists()) {
			return null;
		} else {
			try {
				reader = createNewReader();
				return reader;
			} catch (IOException e) {
				close();
				if (exists()) {
					throw new RuntimeException("Error while creating reader", e);
				} else {
					return null;
				}
			}
		}
	}

	public boolean exists() throws IOException {
		if (deleted) {
			return false;
		} else {
			return fs.exists(path);
		}
	}

	public CheckpointReader createNewReader() throws IOException {
		int retries = 0;
		IOException ioe = null;

		while (retries < 3) {
			try {
				return cf.createReader(fs, path, conf);
			} catch (IOException e) {
				ioe = e;
				retries++;
				if (retries < 3 && LOG.isDebugEnabled()) {
					LOG.debug("Error while creating reader for {} retrying...", path);
				}
			}
		}
		throw ioe;
	}

	public synchronized LookupFile rename(Path newPath) throws IOException {
		if (!deleted) {
			close();
			fs.rename(path, newPath);
			return new LookupFile(newPath, fs, interval, isLocal, conf);
		} else if (fs.exists(newPath)) {
			return new LookupFile(newPath, fs, interval, isLocal, conf);
		} else {
			throw new IOException("File does not exist anymore");
		}

	}

	public synchronized void delete() throws IOException {
		if (!deleted) {
			close();
			if (fs.exists(path)) {
				fs.delete(path, true);
			}
			deleted = true;
		}
	}

	@Override
	public void close() {
		if (reader != null) {
			try {
				reader.close();
			} catch (Exception e) {
			}
			reader = null;
		}
	}

	public long size() throws IOException {
		return fs.getContentSummary(path).getLength();
	}

	@Override
	public String toString() {
		return path.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((fs == null) ? 0 : fs.hashCode());
		result = prime * result + ((path == null) ? 0 : path.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof LookupFile)) {
			return false;
		}
		LookupFile other = (LookupFile) obj;
		if (fs == null) {
			if (other.fs != null) {
				return false;
			}
		} else if (!fs.equals(other.fs)) {
			return false;
		}
		if (path == null) {
			if (other.path != null) {
				return false;
			}
		} else if (!path.equals(other.path)) {
			return false;
		}
		return true;
	}
}
