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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.contrib.streaming.state.hdfs.CheckpointWriter;
import org.apache.flink.contrib.streaming.state.hdfs.KeyScanner;
import org.apache.flink.contrib.streaming.state.hdfs.TFileKvState;
import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import com.google.common.collect.Lists;

public class TfileTest {

	private static void testBasic() throws IOException {

		FileSystem fs = FileSystem.get(new Configuration());

		Path cpFile1 = new Path("/Users/gyulafora/Test", "basic.tfile1");
		Path cpFile2 = new Path("/Users/gyulafora/Test", "basic.tfile2");
		Path cpFile3 = new Path("/Users/gyulafora/Test", "basic.tfile3");

		Map<byte[], byte[]> kvs = new HashMap<>();

		kvs.put(new byte[2], new byte[4]);
		kvs.put(new byte[3], new byte[3]);
		kvs.put(new byte[6], new byte[5]);

		try (CheckpointWriter w = new CheckpointWriter(cpFile1, fs)) {
			w.writeUnsorted(kvs);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		kvs.clear();

		kvs.put(new byte[1], new byte[1]);
		kvs.put(new byte[2], new byte[6]);
		kvs.put(new byte[4], new byte[1]);

		try (CheckpointWriter w = new CheckpointWriter(cpFile2, fs)) {
			w.writeUnsorted(kvs);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		kvs.clear();

		kvs.put(new byte[1], new byte[4]);
		kvs.put(new byte[8], new byte[8]);

		try (CheckpointWriter w = new CheckpointWriter(cpFile3, fs)) {
			w.writeUnsorted(kvs);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		try (KeyScanner r = new KeyScanner(fs, Lists.newArrayList(cpFile3, cpFile2, cpFile1))) {
			System.out.println(Arrays.toString(r.lookup(new byte[8])));
			System.out.println(Arrays.toString(r.lookup(new byte[1])));
			System.out.println(Arrays.toString(r.lookup(new byte[2])));
			System.out.println(Arrays.toString(r.lookup(new byte[6])));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		FileSystem.get(new Configuration()).delete(cpFile1);
		FileSystem.get(new Configuration()).delete(cpFile2);
		FileSystem.get(new Configuration()).delete(cpFile1);
	}

	private static void testFileCreation() throws IOException {
		Path path = new Path("/Users/gyulafora/Test");
		FileSystem fs = FileSystem.get(new Configuration());

		RemoteIterator<LocatedFileStatus> it = fs.listFiles(path, false);

		while (it.hasNext()) {
			Path p = it.next().getPath();
			System.out.println(p.getName());
		}
	}

	private static void testState() throws Exception {
		TFileKvState<Integer, String> state = new TFileKvState<>(FileSystem.get(new Configuration()),
				new Path("/Users/gyulafora/Test"),
				new ArrayList<Path>(),
				IntSerializer.INSTANCE, StringSerializer.INSTANCE, "a", 0);

		state.setCurrentKey(1);

		System.out.println(state.value());

		state.update("b");

		System.out.println(state.value());

		state.setCurrentKey(2);

		System.out.println(state.value());

		System.out.println(state.getCache());
		state.snapshot(0, 100);
		System.out.println(state.getCache());
		state.getCache().clear();
		System.out.println(state.getCache());

		state.setCurrentKey(1);

		System.out.println(state.value());

		state.setCurrentKey(2);

		System.out.println(state.value());

		state.setCurrentKey(1);

		state.update(null);

		KvStateSnapshot<?, ?, ?> s = state.snapshot(0, 101);

		state.getCache().clear();

		System.out.println(state.value());
		System.out.println(state.getKeyScanner());

	}

	private static void benchmark() throws Exception {

		Random rnd = new Random();

		TFileKvState<Integer, String> state = new TFileKvState<>(FileSystem.get(new Configuration()),
				new Path("/Users/gyulafora/Test"),
				new ArrayList<Path>(),
				IntSerializer.INSTANCE, StringSerializer.INSTANCE, "a",
				rnd.nextLong());

		long start = System.nanoTime();

		for (int i = 0; i < 10000000; i++) {
			state.setCurrentKey(rnd.nextInt(100000000));
			state.update(String.valueOf(i));
		}

		state.snapshot(2, rnd.nextLong());

		System.out.println("Time: " + (System.nanoTime() - start) / (1000000 * 1000) + " s");
	}

	public static void main(String[] args) throws Exception {
		// testBasic();
		// testFileCreation();
		// testState();
		benchmark();
	}
}
