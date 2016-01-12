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

package org.apache.flink.contrib.streaming.state;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.hdfs.BloomMapFileCheckpointerFactory;
import org.apache.flink.contrib.streaming.state.hdfs.CheckpointWriter;
import org.apache.flink.contrib.streaming.state.hdfs.CheckpointerFactory;
import org.apache.flink.contrib.streaming.state.hdfs.HdfsCheckpointManager;
import org.apache.flink.contrib.streaming.state.hdfs.HdfsCheckpointManager.Interval;
import org.apache.flink.contrib.streaming.state.hdfs.HdfsKvState;
import org.apache.flink.contrib.streaming.state.hdfs.HdfsKvStateConfig;
import org.apache.flink.contrib.streaming.state.hdfs.HdfsStateBackend;
import org.apache.flink.contrib.streaming.state.hdfs.TFileCheckpointerFactory;
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

	private static void put(KvState<Integer, String, ?> state, Integer key, String value) throws IOException {
		state.setCurrentKey(key);
		state.update(value);
	}

	private static String get(KvState<Integer, String, ?> state, Integer key) throws IOException {
		state.setCurrentKey(key);
		return state.value();
	}

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

	private static void runTest(CheckpointerFactory cf) throws Exception {
		FileSystem fs = FileSystem.get(new Configuration());

		Path cpDir = new Path(TEMP_DIR_PATH, String.valueOf(rnd.nextLong()));
		dirsToRemove.add(cpDir);

		HdfsKvStateConfig conf = new HdfsKvStateConfig(3, 1);
		conf.setCheckpointerFactory(cf);

		HdfsKvState<Integer, String> state = new HdfsKvState<>(
				conf,
				IntSerializer.INSTANCE,
				StringSerializer.INSTANCE, "", 0, 0,
				fs,
				cpDir,
				new HashMap<Interval, Path>());

		put(state, 1, "a");
		put(state, 2, "b");
		put(state, 3, "c");
		put(state, 4, "d");

		assertEquals("a", get(state, 1));
		assertEquals("b", get(state, 2));
		assertEquals("c", get(state, 3));
		assertEquals("d", get(state, 4));

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

		state.getKeyScanner().merge(0, 2);

		KvStateSnapshot<Integer, String, FsStateBackend> s = state.snapshot(1000, 1000);
		Thread.sleep(100);

		Tuple2<Path, Set<Path>> m1 = state.getKeyScanner().merge(0, 1000);
		for (Path pToRemove : m1.f1) {
			fs.delete(pToRemove, true);
		}

		assertEquals("-a", get(state, 1));
		assertEquals("-b", get(state, 2));
		assertEquals("-c", get(state, 3));
		assertEquals("-d", get(state, 4));
		assertEquals("e", get(state, 5));
		assertEquals("f", get(state, 6));

		put(state, 3, "failed");
		put(state, 4, "failed");
		put(state, 5, "failed");
		put(state, 6, "failed");

		state.snapshot(1010, 1010);

		FsStateBackend b = new HdfsStateBackend(cpDir.toString(), conf);
		b.initializeForJob(new DummyEnvironment("", 1, 0));

		state.dispose();
		state = null;
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

		assertEquals("", get(state2, 1));
		assertEquals("cc", get(state2, 3));
		assertEquals("dd", get(state2, 4));

		state2.snapshot(1600, 1600);
		Thread.sleep(100);

		get(state2, 5);
		get(state2, 6);
		get(state2, 7);

		assertEquals("", get(state2, 1));
		assertEquals("cc", get(state2, 3));
		assertEquals("dd", get(state2, 4));

		state2.dispose();

	}

	@Test
	public void test() throws Exception {
		runTest(new TFileCheckpointerFactory());
		runTest(new BloomMapFileCheckpointerFactory());
		// runTest(new MapFileCheckpointerFactory());
	}

	@Test
	public void testMerge() throws Exception {

		testMerge(new TFileCheckpointerFactory());
		testMerge(new BloomMapFileCheckpointerFactory());
	}

	public void testMerge(CheckpointerFactory cf) throws IllegalArgumentException, Exception {

		IntSerializer is = IntSerializer.INSTANCE;
		FileSystem fs = FileSystem.get(new Configuration());
		HdfsKvStateConfig conf = new HdfsKvStateConfig(111, 1);
		conf.setCheckpointerFactory(cf);
		Path cpDir = new Path(TEMP_DIR_PATH, String.valueOf(rnd.nextLong()));
		dirsToRemove.add(cpDir);
		try (HdfsCheckpointManager scanner = new HdfsCheckpointManager(fs, cpDir,
				new HashMap<Interval, Path>(),
				conf)) {
			Map<Integer, Optional<Integer>> kv = new HashMap<>();
			kv.put(0, Optional.of(0));
			kv.put(1, Optional.of(1));

			try (CheckpointWriter w = cf.createWriter(fs, new Path(cpDir + "cp_1"), conf)) {
				w.writeUnsorted(kv.entrySet(), is, is);
			}

			scanner.addNewLookupFile(Interval.point(1), new Path(cpDir + "cp_1"));

			kv.clear();
			kv.put(1, Optional.of(2));

			try (CheckpointWriter w = cf.createWriter(fs, new Path(cpDir + "cp_2"), conf)) {
				w.writeUnsorted(kv.entrySet(), is, is);
			}

			scanner.addNewLookupFile(Interval.point(2), new Path(cpDir + "cp_2"));

			assertEquals(Optional.of(0), scanner.lookupKey(InstantiationUtil.serializeToByteArray(is, 0), is));
			assertEquals(Optional.of(2), scanner.lookupKey(InstantiationUtil.serializeToByteArray(is, 1), is));

			scanner.merge(1, 2);

			assertEquals(Optional.of(0), scanner.lookupKey(InstantiationUtil.serializeToByteArray(is, 0), is));
			assertEquals(Optional.of(2), scanner.lookupKey(InstantiationUtil.serializeToByteArray(is, 1), is));

			assertTrue(fs.exists(new Path(cpDir, "merged_1_2")));

			// TODO: Add more tests here
		}

		Map<Interval, Path> ipMap = new HashMap<>();
		ipMap.put(Interval.point(1), new Path(cpDir + "cp_1"));
		ipMap.put(Interval.point(2), new Path(cpDir + "cp_2"));

		fs.delete(new Path(cpDir + "cp_1"), true);

		try (HdfsCheckpointManager scanner = new HdfsCheckpointManager(fs, cpDir, ipMap,
				conf)) {
			assertEquals(Optional.of(0), scanner.lookupKey(InstantiationUtil.serializeToByteArray(is, 0), is));
			assertEquals(Optional.of(2), scanner.lookupKey(InstantiationUtil.serializeToByteArray(is, 1), is));
		}
	}

}
