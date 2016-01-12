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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.InstantiationUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import com.google.common.base.Optional;

public class HdfsCheckpointManager implements AutoCloseable {

	private final Map<Path, CheckpointReader> openReaders = Collections
			.synchronizedMap(new HashMap<Path, CheckpointReader>());
	private final FileSystem fs;
	private final HdfsKvStateConfig conf;
	private final Path cpParentDir;

	private final SortedMap<Interval, Path> intervalToPath;
	private LinkedList<Interval> sortedIntervals = new LinkedList<>();
	private LinkedList<Path> sortedPaths = new LinkedList<>();

	private final Object mergeLock = new Object();

	public HdfsCheckpointManager(FileSystem fs, Path cpParentDir, Map<Interval, Path> paths, HdfsKvStateConfig conf) {
		this.fs = fs;
		this.conf = conf;
		this.cpParentDir = cpParentDir;
		this.intervalToPath = Collections.synchronizedSortedMap(new TreeMap<>(paths));
		refreshSortedLists();
	}

	public synchronized <V> Optional<V> lookupKey(byte[] key, TypeSerializer<V> valueSerializer) throws IOException {
		for (Path checkpointPath : sortedPaths) {
			CheckpointReader reader = openReaders.get(checkpointPath);
			if (reader == null) {
				if (fs.exists(checkpointPath)) {
					reader = conf.getCheckpointerFactory().createReader(fs, checkpointPath, conf);
				} else {
					Interval lookupInterval = pathToInterval(checkpointPath);
					RemoteIterator<LocatedFileStatus> files = fs.listLocatedStatus(cpParentDir);
					boolean found = false;
					Path correctCpPath = null;
					while (!found && files.hasNext()) {
						Path p = files.next().getPath();
						if (p.getName().startsWith("merged_") || p.getName().startsWith("cp_")) {
							Interval i = pathToInterval(p);
							if (lookupInterval.compareTo(i) == 0) {
								Iterator<Entry<Interval, Path>> eit = intervalToPath.entrySet().iterator();
								while (eit.hasNext()) {
									Entry<Interval, Path> entry = eit.next();
									if (i.compareTo(entry.getKey()) == 0) {
										eit.remove();
										removeReaderAndClose(entry.getValue());
									}
								}
								correctCpPath = p;
								intervalToPath.put(i, p);
								refreshSortedLists();
								found = true;
							}
						}
					}
					if (!found) {
						throw new RuntimeException("Cannot find either checkpoint or merged file");
					}
					reader = conf.getCheckpointerFactory().createReader(fs, correctCpPath, conf);

				}
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

	public <K, V> Tuple2<Double, Double> snapshot(Collection<Entry<K, Optional<V>>> kvs, long timestamp,
			TypeSerializer<K> keySerializer, TypeSerializer<V> valueSerializer) {

		Interval point = Interval.point(timestamp);

		if (!intervalToPath.isEmpty() && point.compareTo(intervalToPath.lastKey()) <= 0) {
			throw new RuntimeException("Lookup file has a timestamp less or equal to the current highest timestamp.");
		}

		Tuple2<Double, Double> kbytesWritten = Tuple2.of(0., 0.);

		if (!kvs.isEmpty()) {
			// We use the timestamp as filename (this is assumed to be
			// increasing between snapshots)
			Path cpFile = new Path(cpParentDir, "cp_" + timestamp);

			// Write the sorted k-v pairs to the new file
			try (CheckpointWriter writer = conf.getCheckpointerFactory().createWriter(fs, cpFile, conf)) {
				kbytesWritten = writer.writeUnsorted(kvs, keySerializer,
						valueSerializer);
			} catch (Exception e) {
				throw new RuntimeException("Could not write checkpoint to disk.", e);
			}

			// Add the new checkpoint file to the scanner for future lookups
			addNewLookupFile(point, cpFile);
		}
		return kbytesWritten;
	}

	private Interval pathToInterval(Path path) {
		String fname = path.getName();
		String[] split = fname.split("_");
		if (split[0].equals("merged")) {
			return Interval.of(Long.parseLong(split[1]), Long.parseLong(split[2]));
		} else {
			return Interval.point(Long.parseLong(split[1]));
		}
	}

	public synchronized void addNewLookupFile(Interval point, Path path) {
		if (!intervalToPath.isEmpty() && point.compareTo(intervalToPath.lastKey()) <= 0) {
			throw new RuntimeException("Lookup file has a timestamp less or equal to the current highest timestamp.");
		}
		intervalToPath.put(point, path);
		sortedIntervals.add(point);
		sortedPaths.addFirst(path);
	}

	private synchronized Set<Path> replaceWithMerged(LinkedList<Interval> mergedIntervals, Path newPath) {

		Set<Path> pathsToRemove = new HashSet<>();

		for (Interval i : mergedIntervals) {
			Path p = intervalToPath.remove(i);
			removeReaderAndClose(p);
			pathsToRemove.add(p);
		}

		intervalToPath.put(Interval.of(mergedIntervals.getFirst().from, mergedIntervals.getLast().to), newPath);

		refreshSortedLists();

		return pathsToRemove;
	}

	private void removeReaderAndClose(Path path) {
		CheckpointReader r = openReaders.remove(path);
		if (r != null) {
			try {
				r.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
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

			if (indexTo - indexFrom > 0) {
				for (int i = indexFrom; i <= indexTo; i++) {
					intervalsToMerge.add(sortedIntervals.get(i));
				}

				Path mergingPath = new Path(cpParentDir,
						"merging_" + intervalsToMerge.getFirst().from + "_" + intervalsToMerge.getLast().to);

				List<Path> pathsToMerge = new ArrayList<>();
				for (Interval i : intervalsToMerge) {
					pathsToMerge.add(intervalToPath.get(i));
				}

				try (CheckpointMerger merger = conf.getCheckpointerFactory().createMerger(fs, pathsToMerge, mergingPath,
						conf)) {
					merger.merge();
				} catch (Exception e) {
					throw new IOException(e);
				}

				Path mergedPath = new Path(cpParentDir,
						"merged_" + intervalsToMerge.getFirst().from + "_" + intervalsToMerge.getLast().to);

				fs.rename(mergingPath, mergedPath);

				return Tuple2.of(mergedPath, intervalsToMerge);
			} else {
				return Tuple2.<Path, LinkedList<Interval>> of(null, new LinkedList<Interval>());
			}

		}
	}

	public Tuple2<Path, Set<Path>> merge(long from, long to) throws IOException {

		// We make sure that 2 merges don't interleave
		synchronized (mergeLock) {
			Tuple2<Path, LinkedList<Interval>> merged = createMerged(from, to);
			if (merged.f0 != null) {
				Set<Path> toRemove = replaceWithMerged(merged.f1, merged.f0);
				return Tuple2.of(merged.f0, toRemove);
			} else {
				return Tuple2.<Path, Set<Path>> of(merged.f0, new HashSet<Path>());
			}
		}
	}

	public Tuple2<Path, Set<Path>> mergeAndRemove(long from, long to) throws IOException {
		Tuple2<Path, Set<Path>> m = merge(from, to);
		deleteFiles(m.f1);
		return m;
	}

	private synchronized void deleteFiles(Set<Path> paths) throws IOException {
		for (Path p : paths) {
			fs.delete(p, true);
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
