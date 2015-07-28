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

package org.apache.flink.test.checkpointing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * A simple test that runs a streaming topology with checkpointing enabled.
 * 
 * The test triggers a failure after a while and verifies that, after
 * completion, the state reflects the "exactly once" semantics.
 */
@SuppressWarnings("serial")
public class PartitionedStateCheckpointingITCase {

	private static final int NUM_TASK_MANAGERS = 2;
	private static final int NUM_TASK_SLOTS = 3;
	private static final int PARALLELISM = NUM_TASK_MANAGERS * NUM_TASK_SLOTS;

	private static ForkableFlinkMiniCluster cluster;

	@BeforeClass
	public static void startCluster() {
		try {
			Configuration config = new Configuration();
			config.setInteger(ConfigConstants.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, NUM_TASK_MANAGERS);
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, NUM_TASK_SLOTS);
			config.setString(ConfigConstants.DEFAULT_EXECUTION_RETRY_DELAY_KEY, "0 ms");
			config.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 12);

			cluster = new ForkableFlinkMiniCluster(config, false);
		} catch (Exception e) {
			e.printStackTrace();
			fail("Failed to start test cluster: " + e.getMessage());
		}
	}

	@AfterClass
	public static void shutdownCluster() {
		try {
			cluster.shutdown();
			cluster = null;
		} catch (Exception e) {
			e.printStackTrace();
			fail("Failed to stop test cluster: " + e.getMessage());
		}
	}

	@Test
	public void runCheckpointedProgram() {

		final long NUM_STRINGS = 10000000L;
		assertTrue("Broken test setup", NUM_STRINGS % 40 == 0);

		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost",
					cluster.getJobManagerRPCPort());
			env.setParallelism(PARALLELISM);
			env.enableCheckpointing(500);
			env.getConfig().disableSysoutLogging();

			DataStream<String> stream1 = env.addSource(new StringGeneratingSourceFunction(NUM_STRINGS / 2));
			DataStream<String> stream2 = env.addSource(new StringGeneratingSourceFunction(NUM_STRINGS / 2));

			stream1.union(stream2)
					.groupBy(new IdentityKeySelector<String>())
					.map(new OnceFailingWordCounter(NUM_STRINGS))
					.keyBy(0)
					.addSink(new CountCounterSink());

			env.execute();

			// verify that we counted exactly right
			for (Long count : OnceFailingWordCounter.allCounts.values()) {
				assertEquals(new Long(NUM_STRINGS / 40), count);
			}
			for (Long count : CountCounterSink.allCounts.values()) {
				assertEquals(new Long(NUM_STRINGS / 40), count);
			}
			
			assertEquals(40, CountCounterSink.allCounts.size());
			assertEquals(40, OnceFailingWordCounter.allCounts.size());
			
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	// --------------------------------------------------------------------------------------------
	// Custom Functions
	// --------------------------------------------------------------------------------------------

	private static class StringGeneratingSourceFunction extends RichSourceFunction<String> implements
			ParallelSourceFunction<String> {

		private final long numElements;

		private OperatorState<Integer> index;
		private int step;

		private volatile boolean isRunning;

		static final long[] counts = new long[PARALLELISM];

		@Override
		public void close() throws IOException {
			counts[getRuntimeContext().getIndexOfThisSubtask()] = index.value();
		}

		StringGeneratingSourceFunction(long numElements) {
			this.numElements = numElements;
		}

		@Override
		public void open(Configuration parameters) throws IOException {
			step = getRuntimeContext().getNumberOfParallelSubtasks();

			index = getRuntimeContext().getOperatorState("index",
					getRuntimeContext().getIndexOfThisSubtask(), false);

			isRunning = true;
		}

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			final Object lockingObject = ctx.getCheckpointLock();

			while (isRunning && index.value() < numElements) {

				synchronized (lockingObject) {
					index.update(index.value() + step);
					ctx.collect(((Integer) (index.value() % 40)).toString());
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}

	private static class OnceFailingWordCounter extends RichMapFunction<String, Tuple2<String, Long>> {

		private static Map<String, Long> allCounts = new ConcurrentHashMap<String, Long>();
		private static volatile boolean hasFailed = false;

		private final long numElements;

		private long failurePos;
		private long count;

		private OperatorState<Long> wordCount;

		OnceFailingWordCounter(long numElements) {
			this.numElements = numElements;
		}

		@Override
		public void open(Configuration parameters) throws IOException {
			long failurePosMin = (long) (0.4 * numElements / getRuntimeContext()
					.getNumberOfParallelSubtasks());
			long failurePosMax = (long) (0.7 * numElements / getRuntimeContext()
					.getNumberOfParallelSubtasks());

			failurePos = (new Random().nextLong() % (failurePosMax - failurePosMin)) + failurePosMin;
			count = 0;
			wordCount = getRuntimeContext().getOperatorState("wc", 0L, true);
		}

		@Override
		public Tuple2<String, Long> map(String value) throws Exception {
			count++;
			if (!hasFailed && count >= failurePos) {
				hasFailed = true;
				throw new Exception("Test Failure");
			}

			long currentCount = wordCount.value() + 1;
			wordCount.update(currentCount);
			allCounts.put(value, currentCount);
			return new Tuple2<String, Long>(value, currentCount);
		}
	}

	private static class CountCounterSink extends RichSinkFunction<Tuple2<String, Long>> {

		private static Map<String, Long> allCounts = new ConcurrentHashMap<String, Long>();

		private OperatorState<Long> counts;

		@Override
		public void open(Configuration parameters) throws IOException {
			counts = getRuntimeContext().getOperatorState("cc", 0L, true);
		}

		@Override
		public void invoke(Tuple2<String, Long> value) throws Exception {
			long currentCount = counts.value() + 1;
			counts.update(currentCount);
			allCounts.put(value.f0, currentCount);

		}
	}
	
	private static class IdentityKeySelector<T> implements KeySelector<T, T> {

		@Override
		public T getKey(T value) throws Exception {
			return value;
		}

	}
}
