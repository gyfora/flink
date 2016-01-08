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
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.contrib.streaming.state.hdfs.CheckpointWriter;
import org.apache.flink.contrib.streaming.state.hdfs.CheckpointerFactory;
import org.apache.flink.contrib.streaming.state.hdfs.HdfsKvState;
import org.apache.flink.contrib.streaming.state.hdfs.HdfsKvStateConfig;
import org.apache.flink.contrib.streaming.state.hdfs.KeyScanner;
import org.apache.flink.contrib.streaming.state.hdfs.MapFileCheckpointerFactory;
import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.flink.util.InstantiationUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

public class TfileTest {

	private static void testBasic2(CheckpointerFactory cf) throws IOException {

		Path cpFile1 = new Path("/Users/gyulafora/Test/" + new Random().nextInt(100000));
		FileSystem fs = cpFile1.getFileSystem(new Configuration());
		
		System.out.println(fs.mkdirs(cpFile1));

		Map<Integer, Optional<Integer>> kvs = new HashMap<>();

		kvs.put(1, Optional.of(1));
		kvs.put(2, Optional.of(2));

		try (CheckpointWriter w = cf.createWriter(fs, cpFile1, null)) {
			w.writeUnsorted(kvs.entrySet(), IntSerializer.INSTANCE, IntSerializer.INSTANCE);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		kvs.clear();

//		kvs.put(1, Optional.of(3));
//
//		try (CheckpointWriter w = cf.createWriter(fs, cpFile1, null)) {
//			w.writeUnsorted(kvs.entrySet(), IntSerializer.INSTANCE, IntSerializer.INSTANCE);
//		} catch (Exception e) {
//			throw new RuntimeException(e);
//		}
//
//		kvs.clear();

		try (KeyScanner r = new KeyScanner(fs, Lists.newArrayList(cpFile1), new HdfsKvStateConfig(1000, 0.5, cf))) {
			System.out.println(r.lookup(InstantiationUtil.serializeToByteArray(IntSerializer.INSTANCE, 1),
					IntSerializer.INSTANCE));
			System.out.println(r.lookup(InstantiationUtil.serializeToByteArray(IntSerializer.INSTANCE, 2),
					IntSerializer.INSTANCE));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

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
		HdfsKvState<Integer, String> state = new HdfsKvState<>(new HdfsKvStateConfig(1000, 0.4),
				FileSystem.get(new Configuration()),
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

	private static void benchmark(CheckpointerFactory cf, String path, int numInserts, int numLookups, int numKeys)
			throws Exception {

		Random rnd = new Random();

		HdfsKvState<Integer, String> state = new HdfsKvState<>(new HdfsKvStateConfig(1000000, 0.5, cf),
				FileSystem.get(new URI("hdfs://172.31.29.148:9000/"), new Configuration()),
				new Path(path),
				new ArrayList<Path>(),
				IntSerializer.INSTANCE, StringSerializer.INSTANCE, "a",
				rnd.nextLong());

		long start = System.nanoTime();

		for (int i = 0; i < numInserts; i++) {
			state.setCurrentKey(rnd.nextInt(numKeys));
			state.update(String.valueOf(i));
		}

		state.snapshot(2, rnd.nextLong());
		System.out.println("Insert Time: " + (System.nanoTime() - start) / (1000000 * 1000) + " s");

		start = System.nanoTime();

		state.getCache().clear();
		for (int i = 0; i < numLookups; i++) {
			state.setCurrentKey(rnd.nextInt(numKeys));
			state.value();
		}

		System.out.println("Lookup Time: " + (System.nanoTime() - start) / (1000000 * 1000) + " s");

	}

	public static void main(String[] args) throws Exception {
		// testBasic2(new TFileCheckpointerFactory());
//		 testBasic2(new MapFileCheckpointerFactory());
		// testFileCreation();
		// testState();
		
		
		benchmark(new MapFileCheckpointerFactory(), "/Users/gyulafora/Test/", 10000000, 1000000, 10000000);

//		Path cpFile1 = new Path("/Users/gyulafora/Test/" + new Random().nextInt(100000));
//		System.out.println(cpFile1.getFileSystem(new Configuration()).mkdirs(cpFile1));
	}
}
