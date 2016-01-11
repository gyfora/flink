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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.InstantiationUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;

public class KeyScanner implements AutoCloseable {

	private final Map<Path, CheckpointReader> openReaders = new HashMap<>();
	private final FileSystem fs;
	private final HdfsKvStateConfig conf;
	private final Path cpParentDir;

	private final SortedMap<Interval, Path> intervalToPath;
	private LinkedList<Interval> sortedIntervals = new LinkedList<>();
	private LinkedList<Path> sortedPaths = new LinkedList<>();

	private final Object mergeLock = new Object();

	public KeyScanner(FileSystem fs, Path cpParentDir, Map<Interval, Path> paths, HdfsKvStateConfig conf) {
		this.fs = fs;
		this.conf = conf;
		this.cpParentDir = cpParentDir;
		this.intervalToPath = Collections.synchronizedSortedMap(new TreeMap<>(paths));
		refreshSortedLists();
	}

	public synchronized <V> Optional<V> lookup(byte[] key, TypeSerializer<V> valueSerializer) throws IOException {
		for (Path checkpointPath : sortedPaths) {
			CheckpointReader reader = openReaders.get(checkpointPath);
			if (reader == null) {
				reader = conf.getCheckpointerFactory().createReader(fs, checkpointPath, conf);
				openReaders.put(checkpointPath, reader);
			}
			byte[] val = reader.lookup(key);
			if (val != null) {
				return val.length > 0
						? Optional.of(InstantiationUtil.deserializeFromByteArray(valueSerializer, val))
						: Optional.<V> absent();
			}
		}
		return Optional.absent();
	}

	public synchronized void addNewLookupFile(long timestamp, Path path) {
		Interval point = Interval.point(timestamp);
		intervalToPath.put(point, path);
		sortedIntervals.add(point);
		sortedPaths.addFirst(path);
	}

	private synchronized void replaceWithMerged(LinkedList<Interval> mergedIntervals, Path newPath) {

		for (Interval i : mergedIntervals) {
			CheckpointReader r = openReaders.remove(intervalToPath.remove(i));
			if (r != null) {
				try {
					r.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		intervalToPath.put(Interval.of(mergedIntervals.getFirst().from, mergedIntervals.getLast().to), newPath);

		refreshSortedLists();
	}

	private void refreshSortedLists() {
		sortedIntervals = new LinkedList<>(intervalToPath.keySet());

		LinkedList<Path> newSortedPaths = new LinkedList<>();
		for (Interval i : sortedIntervals) {
			newSortedPaths.addFirst(intervalToPath.get(i));
		}
		sortedPaths = newSortedPaths;
	}

	public List<Path> getSortedPaths() {
		return sortedPaths;
	}

	public Map<Interval, Path> getIntervalMapping() {
		return intervalToPath;
	}

	private int lookupInterval(long ts) {
		return Collections.binarySearch(sortedIntervals, Interval.point(ts));
	}

	private Tuple2<Path, LinkedList<Interval>> createMerged(long from, long to) throws IOException {
		if (from > to) {
			return createMerged(to, from);
		} else {
			LinkedList<Interval> intervalsToMerge = new LinkedList<>();

			int indexFrom = lookupInterval(from);
			int indexTo = lookupInterval(to);

			for (int i = indexFrom; i <= indexTo; i++) {
				intervalsToMerge.add(sortedIntervals.get(i));
			}

			Path mergedPath = new Path(cpParentDir,
					"merged_" + intervalsToMerge.getFirst().from + "_" + intervalsToMerge.getLast().to);

			List<Path> pathsToMerge = new ArrayList<>();
			for (Interval i : intervalsToMerge) {
				pathsToMerge.add(intervalToPath.get(i));
			}

			try (CheckpointMerger merger = conf.getCheckpointerFactory().createMerger(fs, pathsToMerge, mergedPath,
					conf)) {
				merger.merge();
				System.out.println("Merging: " + pathsToMerge);
			} catch (Exception e) {
				throw new IOException(e);
			}

			return Tuple2.of(mergedPath, intervalsToMerge);
		}
	}

	public Path merge(long from, long to) throws IOException {
		// We make sure that 2 merges don't interleave
		synchronized (mergeLock) {
			Tuple2<Path, LinkedList<Interval>> merged = createMerged(from, to);
			replaceWithMerged(merged.f1, merged.f0);
			return merged.f0;
		}
	}

	@Override
	public void close() throws Exception {
		for (CheckpointReader r : openReaders.values()) {
			r.close();
		}
	}

	@Override
	public String toString() {
		return "KeyScanner: " + getSortedPaths();
	}

	public static class Interval implements Comparable<Interval>, Serializable {
		private static final long serialVersionUID = 1L;
		public final long from;
		public final long to;

		private Interval(long from, long to) {
			this.from = from;
			this.to = to;
		}

		@Override
		public int compareTo(Interval other) {
			if (from > other.to) {
				return 1;
			} else if (to < other.from) {
				return -1;
			} else {
				return 0;
			}
		}

		public static Interval of(long from, long to) {
			return new Interval(from, to);
		}

		public static Interval point(long p) {
			return new Interval(p, p);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (int) (from ^ (from >>> 32));
			result = prime * result + (int) (to ^ (to >>> 32));
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
			if (!(obj instanceof Interval)) {
				return false;
			}
			Interval other = (Interval) obj;
			return compareTo(other) == 0;
		}

		@Override
		public String toString() {
			return "[" + from + ", " + to + "]";
		}

	}
}
