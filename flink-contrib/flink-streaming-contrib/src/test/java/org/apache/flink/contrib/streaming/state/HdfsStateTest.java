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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.contrib.streaming.state.hdfs.BloomMapFileCheckpointerFactory;
import org.apache.flink.contrib.streaming.state.hdfs.CheckpointWriter;
import org.apache.flink.contrib.streaming.state.hdfs.CheckpointerFactory;
import org.apache.flink.contrib.streaming.state.hdfs.HdfsKvState;
import org.apache.flink.contrib.streaming.state.hdfs.HdfsKvStateConfig;
import org.apache.flink.contrib.streaming.state.hdfs.HdfsStateBackend;
import org.apache.flink.contrib.streaming.state.hdfs.KeyScanner;
import org.apache.flink.contrib.streaming.state.hdfs.KeyScanner.Interval;
import org.apache.flink.contrib.streaming.state.hdfs.TFileCheckpointerFactory;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.util.InstantiationUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.google.common.base.Optional;

public class HdfsStateTest {

	private static void put(KvState<Integer, String, ?> state, Integer key, String value) throws IOException {
		state.setCurrentKey(key);
		state.update(value);
	}

	private static String get(KvState<Integer, String, ?> state, Integer key) throws IOException {
		state.setCurrentKey(key);
		return state.value();
	}

	private static void runTest(CheckpointerFactory cf) throws Exception {
		Random rnd = new Random();
		String p = "file:///Users/gyulafora/Test/" + rnd.nextInt();
		HdfsKvStateConfig conf = new HdfsKvStateConfig(3, 1);
		conf.setCheckpointerFactory(cf);

		HdfsKvState<Integer, String> state = new HdfsKvState<>(
				conf,
				IntSerializer.INSTANCE,
				StringSerializer.INSTANCE, "", 0, 0,
				FileSystem.get(new Configuration()),
				new Path(p),
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

		FsStateBackend b = new HdfsStateBackend(p, conf);
		b.initializeForJob(new DummyEnvironment("", 1, 0));

		state = null;
		KvState<Integer, String, ?> state2 = s.restoreState(b, IntSerializer.INSTANCE,
				StringSerializer.INSTANCE, "", Thread.currentThread().getContextClassLoader(), 1500);

		assertEquals("-a", get(state2, 1));
		assertEquals("-b", get(state2, 2));
		assertEquals("-c", get(state2, 3));
		assertEquals("-d", get(state2, 4));
		assertEquals("e", get(state2, 5));
		assertEquals("f", get(state2, 6));

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

		Random rnd = new Random();

		IntSerializer is = IntSerializer.INSTANCE;
		FileSystem fs = FileSystem.get(new Configuration());
		HdfsKvStateConfig conf = new HdfsKvStateConfig(111, 1);
		conf.setCheckpointerFactory(cf);
		String cpDir = "/Users/gyulafora/Test/" + rnd.nextInt() + "/";
		try (KeyScanner scanner = new KeyScanner(fs, new Path(cpDir), new HashMap<Interval, Path>(),
				conf)) {
			Map<Integer, Optional<Integer>> kv = new HashMap<>();
			kv.put(0, Optional.of(0));
			kv.put(1, Optional.of(1));

			try (CheckpointWriter w = cf.createWriter(fs, new Path(cpDir + "1"), conf)) {
				w.writeUnsorted(kv.entrySet(), is, is);
			}

			scanner.addNewLookupFile(1, new Path(cpDir + "1"));

			kv.clear();
			kv.put(1, Optional.of(2));

			try (CheckpointWriter w = cf.createWriter(fs, new Path(cpDir + "2"), conf)) {
				w.writeUnsorted(kv.entrySet(), is, is);
			}

			scanner.addNewLookupFile(2, new Path(cpDir + "2"));

			assertEquals(Optional.of(0), scanner.lookup(InstantiationUtil.serializeToByteArray(is, 0), is));
			assertEquals(Optional.of(2), scanner.lookup(InstantiationUtil.serializeToByteArray(is, 1), is));

			scanner.merge(1, 2);

			assertEquals(Optional.of(0), scanner.lookup(InstantiationUtil.serializeToByteArray(is, 0), is));
			assertEquals(Optional.of(2), scanner.lookup(InstantiationUtil.serializeToByteArray(is, 1), is));

			assertTrue(fs.exists(new Path(cpDir + "merged_1_2")));
		}
	}

}
