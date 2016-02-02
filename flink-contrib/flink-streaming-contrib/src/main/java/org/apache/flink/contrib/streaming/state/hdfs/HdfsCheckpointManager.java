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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.InstantiationUtil;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

public class HdfsCheckpointManager implements AutoCloseable {

	private static final Logger LOG = LoggerFactory.getLogger(HdfsCheckpointManager.class);

	private final Path hdfsParentDir;
	private final Path localParentDir;

	private final FileSystem hadoopFs;
	private final FileSystem localFs;

	private final LinkedList<LookupFile> localLookupFiles = new LinkedList<LookupFile>();
	private final SortedMap<Interval, LookupFile> intervalToLookupFile;

	private final HdfsKvStateConfig conf;

	private final Object mergeLock = new Object();
	private final Object mappingLock = new Object();
	protected final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

	public HdfsCheckpointManager(
			FileSystem hadoopFs,
			Path cpParentDir,
			FileSystem localfs,
			Path tmpDir,
			Map<Interval, URI> intervalMapping,
			HdfsKvStateConfig conf) {

		this.hadoopFs = hadoopFs;
		this.localFs = localfs;
		this.conf = conf;
		this.hdfsParentDir = cpParentDir;
		this.localParentDir = tmpDir;
		this.intervalToLookupFile = toLookupMap(intervalMapping);

		if (LOG.isDebugEnabled()) {
			LOG.debug("HdfsCheckpointManager created: Temp dir:{} Hdfs Dir:{} LookupFile mapping:{}", localParentDir,
					hdfsParentDir, intervalToLookupFile);
		}

		// We schedule an executor that will check whether our lookup files have
		// been deleted so we can release resources.
		executor.scheduleWithFixedDelay(new Runnable() {
			@Override
			public void run() {
				checkFilesForDeletion();
			}
		},
				conf.getFileDeletionCheckFrequencyMillis(),
				conf.getFileDeletionCheckFrequencyMillis(),
				TimeUnit.MILLISECONDS);
	}

	public synchronized <V> Optional<V> lookupKey(byte[] key, TypeSerializer<V> valueSerializer) throws IOException {

		List<Interval> lookupIntervals = null;

		// We need the lock because we are iterating over the keys
		synchronized (mappingLock) {
			// We create a new list so we can release the lock
			lookupIntervals = new ArrayList<>(intervalToLookupFile.keySet());
		}

		// We go through the intervals (they are sorted from most recent to
		// oldest) and return the first value found for the given key.
		for (Interval interval : lookupIntervals) {

			// We get the the lookup file for the interval
			LookupFile lookupFile = null;
			synchronized (mappingLock) {
				lookupFile = intervalToLookupFile.get(interval);
			}

			// CheckPointReader != null iff valid lookup file
			CheckpointReader reader = lookupFile.getReader();

			if (reader == null) {
				// This file is not there anymore, this can happen for various
				// reason: file has been merged, local tmp file has been removed
				// externally.
				if (LOG.isDebugEnabled()) {
					LOG.debug("Can't find lookup file {}, trying to find merged or remote file for {}",
							lookupFile.path,
							interval);
				}

				LookupFile newLookupFile = findLocalOrRemote(interval);

				if (newLookupFile != null) {
					// If new file was found, replace the reader
					reader = newLookupFile.getReader();
				}

				if (reader == null) {
					throw new RuntimeException("Invalid lookup file.");
				}
			}

			// We try to find value in this interval
			byte[] val = reader.lookup(key);
			if (val != null) {
				// We don't need to look further
				return val.length > 0
						? Optional.of(InstantiationUtil.deserializeFromByteArray(valueSerializer, val))
						: Optional.<V> absent();
			}
		}

		return Optional.absent();
	}

	private LookupFile findLocalOrRemote(Interval interval) throws FileNotFoundException, IOException {
		LookupFile newLookupFile = null;

		// Try to find merged file locally
		if (localFs.exists(localParentDir)) {
			newLookupFile = findMergedOrRemoteFile(interval, localFs, localParentDir);
		}

		// Try to find merged file remotely
		if (newLookupFile == null) {
			newLookupFile = findMergedOrRemoteFile(interval, hadoopFs, hdfsParentDir);
		}

		if (newLookupFile == null) {
			throw new RuntimeException("Could not found local or remote file");
		}

		return newLookupFile;
	}

	private LookupFile findMergedOrRemoteFile(Interval interval, FileSystem fs, Path parentDir)
			throws FileNotFoundException, IOException {

		if (LOG.isDebugEnabled()) {
			LOG.debug("Trying to find {} file for {} in {}", fs.equals(localFs) ? "local" : "remote", interval,
					parentDir);
		}

		LookupFile mergedFile = null;
		// List the all the checkpoint or merged files
		FileStatus[] files = fs.listStatus(parentDir, new PathFilter() {

			@Override
			public boolean accept(Path p) {
				return p.getName().startsWith("merged_") || p.getName().startsWith("cp_");
			}
		});

		// We iterate until we find the file or no more files left
		for (FileStatus status : files) {
			try {
				Path path = status.getPath();
				Interval mergedInterval = Interval.pathToInterval(path);

				if (interval.overlapsWith(mergedInterval)) {

					if (LOG.isDebugEnabled()) {
						LOG.debug("Found merged file for {}: {}", interval, path);
					}

					boolean shouldCopyBack = fs.equals(hadoopFs) && conf.shouldCopyBackFromHdfs();
					boolean belowSizeLimit = shouldCopyBack
							&& getFileOrDirSize(fs, path) < conf.getCopySizeLimit();

					if (shouldCopyBack && !belowSizeLimit && LOG.isDebugEnabled()) {
						LOG.debug("File size ({}) exceeds copy-back limit ({}). Keeping on hdfs.",
								getFileOrDirSize(fs, path),
								conf.getCopySizeLimit());
					}

					// Additionally we copy the found file back from hdfs if
					// it is set in the configuration and is under the copy
					// size limit
					if (shouldCopyBack && belowSizeLimit) {
						Path localFile = new Path(localParentDir, path.getName());
						fs.copyToLocalFile(path, localFile);
						mergedFile = new LookupFile(localFile, localFs, true, conf);
						if (LOG.isDebugEnabled()) {
							LOG.debug("Copied from hdfs to local dir: {} Using local file...", localFile);
						}
					} else {
						mergedFile = new LookupFile(path, fs, mergedInterval, fs.equals(localFs), conf);
					}

					// We replace all the lookups file currently held for
					// that interval with new file and delete the replaced
					// files

					List<LookupFile> mergedFiles = getLookupFilesForInterval(mergedInterval);

					synchronized (mappingLock) {
						asyncDelete(replaceWithMerged(mergedFiles, mergedFile));
					}
				}
			} catch (FileNotFoundException e) {
				// Sometimes the RemoteIterator contains file
				// that have already been deleted, so we need to
				// disregard this exception
			}
		}
		return mergedFile;

	}

	private static long getFileOrDirSize(FileSystem fs, Path p) throws IOException {
		return fs.getContentSummary(p).getLength();
	}

	public <K, V> Tuple2<Double, Double> snapshotToLocal(Collection<Entry<K, Optional<V>>> kvs, long timestamp,
			TypeSerializer<K> keySerializer, TypeSerializer<V> valueSerializer) {

		// We measure time for performance debugging
		long startTime = System.nanoTime();

		Interval point = Interval.point(timestamp);

		if (!intervalToLookupFile.isEmpty() && point.compareTo(intervalToLookupFile.firstKey()) >= 0) {
			throw new RuntimeException("Lookup file has a timestamp less or equal to the current highest timestamp.");
		}

		Tuple2<Double, Double> kbytesWritten = Tuple2.of(0., 0.);

		if (!kvs.isEmpty()) {
			// We use the timestamp as filename (this is assumed to be
			// increasing between snapshots)
			Path cpFile = new Path(localParentDir, "cp_" + timestamp);

			if (LOG.isDebugEnabled()) {
				LOG.debug("Snapshotting modified state to {}", cpFile);
			}

			// Write the sorted k-v pairs to the new file
			try (CheckpointWriter writer = conf.getCheckpointerFactory().createWriter(localFs, cpFile, conf)) {
				kbytesWritten = writer.writeUnsorted(kvs, keySerializer, valueSerializer);
			} catch (Exception e) {
				throw new RuntimeException("Could not write checkpoint to disk.", e);
			}

			addLocalLookupFile(new LookupFile(cpFile, localFs, point, true, conf));
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug(
					"SnapshotToLocal completed: (state, numStates, keyKbsWritten, valueKbsWritten, timeMillis) = ({}, {}, {}, {}, {})",
					localParentDir, kvs.size(), kbytesWritten.f0, kbytesWritten.f1,
					(System.nanoTime() - startTime) / 1000000);
		}

		return kbytesWritten;
	}

	protected void addLocalLookupFile(LookupFile lookupFile) {
		// We can't add new local files while we are merging the current ones
		synchronized (mergeLock) {
			// This will affect the iterators so we lock
			synchronized (mappingLock) {
				localLookupFiles.add(0, lookupFile);
				intervalToLookupFile.put(lookupFile.interval, lookupFile);
				LOG.debug("New local lookup file added: {}", lookupFile);
			}
		}
	}

	public synchronized void checkFilesForDeletion() {
		LOG.debug("Checking lookup files for deletion...");
		synchronized (mergeLock) {
			List<LookupFile> lookupFiles = null;
			synchronized (mappingLock) {
				lookupFiles = new ArrayList<>(intervalToLookupFile.values());
			}
			for (LookupFile lf : lookupFiles) {
				try {
					if (!lf.exists()) {
						if (LOG.isDebugEnabled()) {
							LOG.debug("Found deleted lookup file {}, cleaning up...", lf.path);
						}
						lf.close();
						LookupFile newLookupFile = findLocalOrRemote(lf.interval);

						synchronized (mappingLock) {
							replaceWithMerged(Collections.singletonList(lf), newLookupFile);
						}
						// Grab resources
						newLookupFile.getReader();
					} else {
						// Grab resources
						lf.getReader();
					}
					// We call getReader on the lookupfiles (or on the new one
					// if it was deleted) to grab the resources making sure they
					// are not accidentally deleted before a merge operation.
				} catch (IOException e) {
					LOG.debug("Error while cleaning up deleted files: {}", e.getMessage());
				}
			}
		}
	}

	public LookupFile mergeLocalFilesToHdfs() throws IOException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Merging local files {} to hdfs", localLookupFiles);
		}

		synchronized (mergeLock) {

			// We merge the intermediate lookup files to either locally or
			// directly to hdfs depending on the config
			LookupFile mergedFile = mergeFiles(localLookupFiles,
					conf.shouldKeepLocalFiles() ? localParentDir : hdfsParentDir);

			if (mergedFile != null) {

				// If we merged locally we need to copy to hdfs for ft
				if (conf.shouldKeepLocalFiles()) {
					hadoopFs.copyFromLocalFile(mergedFile.path, new Path(hdfsParentDir, mergedFile.path.getName()));
				}

				// Finally we replace the lookup files with the newly merged one
				// and remove the others
				synchronized (mappingLock) {
					asyncDelete(replaceWithMerged(new ArrayList<>(localLookupFiles), mergedFile));
				}

				localLookupFiles.clear();

			}

			return mergedFile;
		}

	}

	public LookupFile mergeCheckpoints(long from, long to) throws IOException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Merging checkpoint files between {} and {}", from, to);
		}

		// We make sure that we don't try to merge already deleted files
		checkFilesForDeletion();

		synchronized (mergeLock) {
			LinkedList<LookupFile> filesToMerge = getLookupFilesForInterval(Interval.closed(from, to));

			// We only merge if there are more than 1 files
			if (filesToMerge.size() > 1) {

				boolean mergeToLocal = conf.shouldKeepLocalFiles();
				if (mergeToLocal) {
					LOG.debug("Merging to local...");
				}

				LookupFile mergedFile = mergeFiles(filesToMerge, mergeToLocal ? localParentDir : hdfsParentDir);

				// If we merged to local fs we need to copy to hdfs
				if (mergedFile.isLocal) {
					LOG.debug("Copying local file to HDFS...");
					hadoopFs.copyFromLocalFile(mergedFile.path, new Path(hdfsParentDir, mergedFile.path.getName()));
				}

				synchronized (mappingLock) {
					final Set<LookupFile> toRemove = replaceWithMerged(filesToMerge, mergedFile);
					// We need to be slightly more clever with file removal in
					// this case. If we merged local checkpoint files we need to
					// delete the hdfs counterparts as well.
					for (LookupFile lf : new ArrayList<>(toRemove)) {
						if (lf.isLocal) {
							LookupFile rf = new LookupFile(new Path(hdfsParentDir, lf.path.getName()), hadoopFs, false,
									conf);
							toRemove.add(rf);
							if (LOG.isDebugEnabled()) {
								LOG.debug("Remote file {} added for deletion.", rf.path);
							}
						}
					}
					asyncDelete(toRemove);
				}

				return mergedFile;
			} else {
				LOG.debug("Given merge interval only contains 1 file.");
				return null;
			}
		}
	}

	private LinkedList<LookupFile> getLookupFilesForInterval(Interval mergeInterval) throws IOException {
		LinkedList<LookupFile> files = new LinkedList<>();

		// We need the lock because we are iterating over the entries
		synchronized (mappingLock) {
			for (Entry<Interval, LookupFile> e : intervalToLookupFile.entrySet()) {
				if (e.getKey().overlapsWith(mergeInterval)) {
					files.add(e.getValue());
				}
			}
		}

		return files;
	}

	@SuppressWarnings("resource")
	private LookupFile mergeFiles(List<LookupFile> sortedLookupFiles, Path mergeDir) throws IOException {

		// Timer for logging
		long startTime = System.nanoTime();

		LookupFile ret = null;
		boolean mergingToHdfs = mergeDir.equals(hdfsParentDir);

		// We have more than one files (actually merging)
		if (sortedLookupFiles.size() > 1) {

			// We create the merged file by extracting the interval endpoints
			Interval mergedInterval = Interval.closed(sortedLookupFiles.get(sortedLookupFiles.size() - 1).interval.from,
					sortedLookupFiles.get(0).interval.to);

			// First create "merging_from_to" file which will later be renamed
			// to "merged_from_to". This avoids complications when searching for
			// merged files later.
			LookupFile mergingFile = new LookupFile(
					new Path(mergeDir, "merging_" + mergedInterval.from + "_" + mergedInterval.to),
					mergingToHdfs ? hadoopFs : localFs,
					mergedInterval, !mergingToHdfs, conf);

			// Execute merge
			try (CheckpointMerger merger = conf.getCheckpointerFactory().createMerger(sortedLookupFiles, mergingFile,
					conf)) {
				merger.merge();
			} catch (Exception e) {
				throw new IOException(e);
			}

			// Rename merging file to merged
			ret = mergingFile.rename(new Path(mergeDir, "merged_" + mergedInterval.from + "_" + mergedInterval.to));

		} else if (sortedLookupFiles.size() == 1) {
			// We only have 1 file but we might need to copy it
			LookupFile lf = sortedLookupFiles.get(0);

			// If we are merging a local file to hdfs , or merging an hdfs file
			// to local copy it
			if (mergingToHdfs && lf.isLocal) {
				LookupFile copied = new LookupFile(new Path(hdfsParentDir, lf.path.getName()), hadoopFs,
						lf.interval, false, conf);

				hadoopFs.copyFromLocalFile(lf.path, copied.path);
				ret = copied;
			} else if (!mergingToHdfs && !lf.isLocal) {
				LookupFile copied = new LookupFile(new Path(localParentDir, lf.path.getName()), localFs,
						lf.interval, true, conf);

				hadoopFs.copyToLocalFile(lf.path, copied.path);
				ret = copied;
			} else {
				ret = lf;
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Merge completed: (targetDir, numFiles, timeMillis) = ({}, {}, {})",
					mergeDir, sortedLookupFiles.size(), (System.nanoTime() - startTime) / 1000000);
		}

		return ret;
	}

	/**
	 * Not thread-safe, lock on mappingLock before calling.
	 */
	private Set<LookupFile> replaceWithMerged(List<LookupFile> sortedLookupFiles, LookupFile newFile) {

		if (sortedLookupFiles.size() == 0
				|| (sortedLookupFiles.size() == 1 && sortedLookupFiles.get(0).equals(newFile))) {
			// We return empty set if we don't need to replace anything
			return new HashSet<>();
		}

		// We remove and close the lookup files that will be replaced
		for (LookupFile lf : sortedLookupFiles) {
			intervalToLookupFile.remove(lf.interval);
			localLookupFiles.remove(lf);
			lf.close();
		}

		// We extract the merged interval (this will replace the removed ones)
		Interval mergedInterval = Interval.closed(sortedLookupFiles.get(sortedLookupFiles.size() - 1).interval.from,
				sortedLookupFiles.get(0).interval.to);

		// We add the new file
		intervalToLookupFile.put(mergedInterval, newFile);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Replaced lookup files with merged file: {} -> {}\nCurrent mapping: {}", sortedLookupFiles,
					newFile, intervalToLookupFile);
		}

		// Now all the lookup files can be deleted
		return new HashSet<>(sortedLookupFiles);
	}

	public void asyncDelete(final Collection<LookupFile> files) {
		if (!files.isEmpty()) {
			executor.submit(new Runnable() {
				@Override
				public void run() {
					try {
						for (LookupFile f : files) {
							f.delete();
						}
						if (!files.isEmpty() && LOG.isDebugEnabled()) {
							LOG.debug("Deleted lookup files: {}", files);
						}
					} catch (IOException e) {
					}
				}
			});
		}
	}

	public Map<Interval, URI> getIntervalMapping() {
		Map<Interval, URI> out = new TreeMap<>();
		synchronized (mappingLock) {
			for (Entry<Interval, LookupFile> e : intervalToLookupFile.entrySet()) {
				out.put(e.getKey(), e.getValue().path.toUri());
			}
		}
		return out;
	}

	public Map<Interval, URI> getMappingForSnapshot() {
		synchronized (mergeLock) {
			if (localLookupFiles.isEmpty()) {
				return getIntervalMapping();
			} else {
				throw new RuntimeException("Unmerged local files");
			}
		}
	}

	private SortedMap<Interval, LookupFile> toLookupMap(Map<Interval, URI> paths) {
		SortedMap<Interval, LookupFile> lookupMap = new TreeMap<>();

		for (Entry<Interval, URI> e : paths.entrySet()) {
			Path cpPath = new Path(e.getValue());
			Path parent = cpPath.getParent();
			if (parent.equals(hdfsParentDir)) {
				lookupMap.put(e.getKey(), new LookupFile(cpPath, hadoopFs, e.getKey(), false, conf));
			} else if (parent.equals(localParentDir)) {
				lookupMap.put(e.getKey(), new LookupFile(cpPath, localFs, e.getKey(), true, conf));
			} else {
				throw new RuntimeException("Path parent directory does not match local or hdfs checkpoint dir.");
			}
		}

		return lookupMap;
	}

	public Path getCheckpointDir() {
		return hdfsParentDir;
	}

	public Path getLocalTmpDir() {
		return localParentDir;
	}

	@Override
	public String toString() {
		return "CpManager: " + intervalToLookupFile;
	}

	@Override
	public void close() {
		synchronized (mappingLock) {
			for (LookupFile lf : intervalToLookupFile.values()) {
				lf.close();
			}
		}
		executor.shutdownNow();
	}
}
