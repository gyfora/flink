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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.contrib.streaming.state.CompactionStrategy;
import org.apache.flink.contrib.streaming.state.hdfs.mapfile.BloomMapFileCheckpointerFactory;
import org.apache.flink.contrib.streaming.state.hdfs.tfile.TFileCheckpointerFactory;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.util.InstantiationUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Test;

import com.google.common.base.Optional;

public class HdfsStateTest {

	private static Random rnd = new Random();
	private static List<Path> dirsToRemove = Collections.synchronizedList(new ArrayList<Path>());
	private static final Path TEMP_DIR_PATH = new Path(new File(System.getProperty("java.io.tmpdir")).toURI());

	@AfterClass
	public static void removeTempDirs() throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		for (Path p : dirsToRemove) {
			try {
				fs.delete(p, true);
			} catch (Exception e) {
			}
		}
	}

	@Test
	public void testKvState() throws Exception {
		runTest(new TFileCheckpointerFactory().setIndexInterval(10), false, false);
		runTest(new TFileCheckpointerFactory().setIndexInterval(-1), true, false);
		runTest(new TFileCheckpointerFactory(), true, true);

		runTest(new BloomMapFileCheckpointerFactory().setIndexInterval(10), false, false);
		runTest(new BloomMapFileCheckpointerFactory().setIndexInterval(-1), true, false);
		runTest(new BloomMapFileCheckpointerFactory(), true, true);
	}

	@Test
	public void testMerge() throws Exception {
		testMerge(new TFileCheckpointerFactory().setIndexInterval(-1), false);
		testMerge(new BloomMapFileCheckpointerFactory(), false);
		testMerge(new TFileCheckpointerFactory(), true);
		testMerge(new BloomMapFileCheckpointerFactory().setIndexInterval(-1), true);
	}

	private static void put(KvState<Integer, String, ?> state, Integer key, String value) throws IOException {
		state.setCurrentKey(key);
		state.update(value);
	}

	private static String get(KvState<Integer, String, ?> state, Integer key) throws IOException {
		state.setCurrentKey(key);
		return state.value();
	}

	private static void waitForExecutingThread(ExecutorService executor)
			throws InterruptedException, ExecutionException {
		executor.submit(new Callable<Void>() {

			@Override
			public Void call() throws Exception {
				return null;
			}
		}).get();
	}

	private static void runTest(CheckpointerFactory cf, boolean keepLocal, boolean copyBack)
			throws Exception {

		Path cpDir = new Path(TEMP_DIR_PATH, String.valueOf(rnd.nextLong()));
		Path tmpDir = new Path(cpDir, "tmp");
		dirsToRemove.add(cpDir);

		HdfsKvStateConfig conf = new HdfsKvStateConfig(3, 1).setCheckpointerFactory(cf);
		if (keepLocal) {
			conf.keepLocalFilesAfterSnapshot();
		}
		if (copyBack) {
			conf.copyBackFromHdfs(100000000);
		}
		if (rnd.nextBoolean()) {
			conf.enableBloomFilter(1000, 0.1);
		}

		HdfsStateBackend backend = new HdfsStateBackend(cpDir.toString(), tmpDir.toString(), conf);
		backend.initializeForJob(new DummyEnvironment("test-task", 2, 1));

		FileSystem fs = backend.getLocalFileSystem();

		final HdfsKvState<Integer, String> state = new HdfsKvState<>(
				backend,
				"test",
				conf,
				IntSerializer.INSTANCE,
				StringSerializer.INSTANCE, "", 0, 0,
				fs,
				cpDir,
				fs,
				tmpDir,
				new HashMap<Interval, URI>());

		put(state, 1, "a");
		put(state, 2, "b");
		put(state, 3, "c");
		put(state, 4, "d");

		assertEquals("a", get(state, 1));
		assertEquals("b", get(state, 2));
		assertEquals("c", get(state, 3));
		assertEquals("d", get(state, 4));
		assertEquals("", get(state, 5));
		assertEquals("", get(state, 6));
		assertEquals("", get(state, 7));

		put(state, 1, "-a");
		put(state, 2, "-b");
		put(state, 3, "-c");
		put(state, 4, "-d");
		put(state, 5, "e");
		put(state, 6, "f");

		assertEquals("-a", get(state, 1));
		assertEquals("-b", get(state, 2));
		assertEquals("-c", get(state, 3));
		assertEquals("-d", get(state, 4));
		assertEquals("e", get(state, 5));
		assertEquals("f", get(state, 6));

		KvStateSnapshot<Integer, String, FsStateBackend> s = state.snapshot(1000, 1000);

		assertTrue(fs.exists(new Path(cpDir, "merged_0_4")));
		assertEquals(conf.shouldKeepLocalFiles(), fs.exists(new Path(tmpDir, "merged_0_4")));

		waitForExecutingThread(state.getCheckpointManager().executor);

		state.getCheckpointManager().checkFilesForDeletion();

		assertFalse(fs.exists(new Path(tmpDir, "cp_1")));
		assertFalse(fs.exists(new Path(tmpDir, "cp_2")));
		assertFalse(fs.exists(new Path(tmpDir, "cp_3")));

		URI uri1 = state.getCheckpointManager().getIntervalMapping().get(Interval.point(1));
		URI uri2 = state.getCheckpointManager().getIntervalMapping().get(Interval.point(3));

		URI expected = new Path(!conf.shouldKeepLocalFiles() ? cpDir : tmpDir, "merged_0_4").toUri();

		assertEquals(expected, uri1);
		assertEquals(expected, uri2);

		fs.delete(new Path(tmpDir, "cp_1"), true);
		fs.delete(new Path(tmpDir, "cp_2"), true);
		fs.delete(new Path(tmpDir, "cp_3"), true);

		assertEquals("-a", get(state, 1));
		assertEquals("-b", get(state, 2));
		assertEquals("-c", get(state, 3));
		assertEquals("-d", get(state, 4));
		assertEquals("e", get(state, 5));
		assertEquals("f", get(state, 6));

		URI uri3 = state.getCheckpointManager().getIntervalMapping().get(Interval.point(1));
		URI uri4 = state.getCheckpointManager().getIntervalMapping().get(Interval.point(3));

		URI expected2 = new Path(conf.shouldKeepLocalFiles() ? tmpDir : cpDir, "merged_0_4").toUri();

		assertEquals(expected2, uri3);
		assertEquals(expected2, uri4);

		put(state, 3, "failed");
		put(state, 4, "failed");
		put(state, 5, "failed");
		put(state, 6, "failed");

		state.snapshot(1010, 1010);

		FsStateBackend b = new HdfsStateBackend(cpDir.toString(), tmpDir.toString(), conf);
		b.initializeForJob(new DummyEnvironment("", 1, 0));

		state.dispose();
		HdfsKvState<Integer, String> state2 = (HdfsKvState<Integer, String>) s.restoreState(b, IntSerializer.INSTANCE,
				StringSerializer.INSTANCE, "", Thread.currentThread().getContextClassLoader(), 1500);

		assertEquals("-a", get(state2, 1));
		assertEquals("-b", get(state2, 2));
		assertEquals("-c", get(state2, 3));
		assertEquals("-d", get(state2, 4));
		assertEquals("e", get(state2, 5));
		assertEquals("f", get(state2, 6));

		put(state2, 1, "a");
		put(state2, 3, "cc");
		put(state2, 4, "dd");
		put(state2, 1, null);
		put(state2, 10, "a");
		put(state2, 11, "a");

		assertEquals("", get(state2, 1));
		assertEquals("cc", get(state2, 3));
		assertEquals("dd", get(state2, 4));

		state2.snapshot(1600, 1600);

		get(state2, 5);
		get(state2, 6);
		get(state2, 7);

		assertEquals("", get(state2, 1));
		assertEquals("cc", get(state2, 3));
		assertEquals("dd", get(state2, 4));

		state2.dispose();

	}

	public void testMerge(CheckpointerFactory cf, boolean keepLocal)
			throws IllegalArgumentException, Exception {

		IntSerializer is = IntSerializer.INSTANCE;
		FileSystem cpFs = FileSystem.get(new Configuration());
		FileSystem localFs = FileSystem.get(new Configuration());

		HdfsKvStateConfig conf = new HdfsKvStateConfig(111, 1);
		conf.setCheckpointerFactory(cf);
		if (keepLocal) {
			conf.keepLocalFilesAfterSnapshot();
		}
		Path cpDir = new Path(TEMP_DIR_PATH, String.valueOf(rnd.nextLong()));
		Path tmpDir = new Path(cpDir, "tmp");
		localFs.mkdirs(tmpDir);

		dirsToRemove.add(cpDir);
		try (HdfsCheckpointManager checkpointManager = new HdfsCheckpointManager(cpFs, cpDir, localFs, tmpDir,
				new HashMap<Interval, URI>(),
				conf)) {
			Map<Integer, Optional<Integer>> kv = new HashMap<>();
			kv.put(0, Optional.of(0));
			kv.put(1, Optional.of(1));

			try (CheckpointWriter w = cf.createWriter(cpFs, new Path(cpDir + "cp_1"), conf)) {
				w.writeUnsorted(kv.entrySet(), is, is);
			}

			checkpointManager
					.addLocalLookupFile(new LookupFile(new Path(cpDir + "cp_1"), cpFs, Interval.point(1), false, conf));

			kv.clear();
			kv.put(1, Optional.of(2));

			try (CheckpointWriter w = cf.createWriter(cpFs, new Path(cpDir + "cp_2"), conf)) {
				w.writeUnsorted(kv.entrySet(), is, is);
			}

			checkpointManager
					.addLocalLookupFile(new LookupFile(new Path(cpDir + "cp_2"), cpFs, Interval.point(2), false, conf));

			assertEquals(Optional.of(0),
					checkpointManager.lookupKey(InstantiationUtil.serializeToByteArray(is, 0), is));
			assertEquals(Optional.of(2),
					checkpointManager.lookupKey(InstantiationUtil.serializeToByteArray(is, 1), is));

			checkpointManager.mergeCheckpoints(1, 2);

			assertEquals(Optional.of(0),
					checkpointManager.lookupKey(InstantiationUtil.serializeToByteArray(is, 0), is));
			assertEquals(Optional.of(2),
					checkpointManager.lookupKey(InstantiationUtil.serializeToByteArray(is, 1), is));

			assertTrue(cpFs.exists(new Path(cpDir, "merged_1_2")));
		}

		Map<Interval, URI> ipMap = new HashMap<>();
		ipMap.put(Interval.point(1), new Path(cpDir, "cp_1").toUri());
		ipMap.put(Interval.point(2), new Path(cpDir, "cp_2").toUri());

		// cpFs.delete(new Path(cpDir + "cp_1"), true);
		assertTrue(!cpFs.exists(new Path(cpDir, "cp_1")));

		try (HdfsCheckpointManager scanner = new HdfsCheckpointManager(cpFs, cpDir, localFs, tmpDir, ipMap,
				conf)) {
			assertEquals(Optional.of(0), scanner.lookupKey(InstantiationUtil.serializeToByteArray(is, 0), is));
			assertEquals(Optional.of(2), scanner.lookupKey(InstantiationUtil.serializeToByteArray(is, 1), is));
		}
	}

	@Test
	public void testWithCompaction() throws Exception {
		runTest(new TFileCheckpointerFactory(), true, true, 2, false);
		runTest(new TFileCheckpointerFactory(), true, true, 2, true);
	}

	private static void runTest(CheckpointerFactory cf, boolean keepLocal, boolean copyBack, int compactEvery,
			boolean compactFromBeginning)
					throws Exception {

		Path cpDir = new Path(TEMP_DIR_PATH, String.valueOf(rnd.nextLong()));
		Path tmpDir = new Path(cpDir, "tmp");
		dirsToRemove.add(cpDir);

		HdfsKvStateConfig conf = new HdfsKvStateConfig(3, 1).setCheckpointerFactory(cf);
		if (keepLocal) {
			conf.keepLocalFilesAfterSnapshot();
		}
		if (copyBack) {
			conf.copyBackFromHdfs(100000000);
		}
		if (rnd.nextBoolean()) {
			conf.enableBloomFilter(1000, 0.1);
		}
		if (compactEvery > 0) {
			conf.setCompactionStrategy(new CompactionStrategy(compactEvery, compactFromBeginning));
		}

		HdfsStateBackend backend = new HdfsStateBackend(cpDir.toString(), tmpDir.toString(), conf);
		backend.initializeForJob(new DummyEnvironment("test-task", 2, 1));

		FileSystem fs = backend.getLocalFileSystem();

		HdfsKvState<Integer, String> state = new HdfsKvState<>(
				backend,
				"test",
				conf,
				IntSerializer.INSTANCE,
				StringSerializer.INSTANCE, "", 0, 0,
				fs,
				cpDir,
				fs,
				tmpDir,
				new HashMap<Interval, URI>());

		put(state, 1, "a");
		put(state, 2, "b");
		put(state, 3, "c");
		put(state, 4, "d");

		state.snapshot(800, 800);
		state.notifyCheckpointComplete(800);
		waitForExecutingThread(state.executor);

		assertEquals("a", get(state, 1));
		assertEquals("b", get(state, 2));
		assertEquals("c", get(state, 3));
		assertEquals("d", get(state, 4));
		assertEquals("", get(state, 5));
		assertEquals("", get(state, 6));
		assertEquals("", get(state, 7));

		put(state, 1, "-a");
		put(state, 2, "-b");
		put(state, 3, "-c");
		state.snapshot(900, 900);
		state.notifyCheckpointComplete(900);
		waitForExecutingThread(state.executor);
		put(state, 4, "-d");
		put(state, 5, "e");
		put(state, 6, "f");

		assertEquals("-a", get(state, 1));
		assertEquals("-b", get(state, 2));
		assertEquals("-c", get(state, 3));
		assertEquals("-d", get(state, 4));
		assertEquals("e", get(state, 5));
		assertEquals("f", get(state, 6));

		state.snapshot(1000, 1000);
		state.notifyCheckpointComplete(1000);
		waitForExecutingThread(state.executor);

		assertEquals("-a", get(state, 1));
		assertEquals("-b", get(state, 2));
		assertEquals("-c", get(state, 3));
		assertEquals("-d", get(state, 4));
		assertEquals("e", get(state, 5));
		assertEquals("f", get(state, 6));

		put(state, 1, "a");
		put(state, 3, "cc");
		put(state, 4, "dd");
		state.snapshot(1100, 1100);
		state.notifyCheckpointComplete(1100);
		waitForExecutingThread(state.executor);
		put(state, 1, null);
		put(state, 10, "a");
		put(state, 11, "a");

		state.snapshot(1200, 1200);
		state.notifyCheckpointComplete(1200);
		waitForExecutingThread(state.executor);

		assertEquals("", get(state, 1));
		assertEquals("cc", get(state, 3));
		assertEquals("dd", get(state, 4));

		get(state, 5);
		get(state, 6);
		get(state, 7);

		assertEquals("", get(state, 1));
		assertEquals("cc", get(state, 3));
		assertEquals("dd", get(state, 4));

		state.dispose();

	}

}
