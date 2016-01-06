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

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.hdfs.TFileStateBackend;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.test.checkpointing.PartitionedStateCheckpointingITCase.IdentityKeySelector;
import org.apache.flink.test.checkpointing.PartitionedStateCheckpointingITCase.NonSerializableLong;
import org.apache.flink.test.checkpointing.StreamFaultToleranceTestBase;

@SuppressWarnings("serial")
public class TfileStateCheckpointingTest extends StreamFaultToleranceTestBase {
	final long NUM_STRINGS = 1_000_000L;
	final static int NUM_KEYS = 100;

	@Override
	public void testProgram(StreamExecutionEnvironment env) {
		env.enableCheckpointing(500);

		try {
			env.setStateBackend(new TFileStateBackend("file:///Users/gyulafora/Test", new KvStateConfig(5000, 0.5f)));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		DataStream<Integer> stream1 = env.addSource(new IntGeneratingSourceFunction(NUM_STRINGS / 2));
		DataStream<Integer> stream2 = env.addSource(new IntGeneratingSourceFunction(NUM_STRINGS / 2));

		stream1.union(stream2).keyBy(new IdentityKeySelector<Integer>()).map(new OnceFailingPartitionedSum(NUM_STRINGS))
				.keyBy(0).addSink(new CounterSink());
	}

	@Override
	public void postSubmit() {
		// verify that we counted exactly right
		for (Entry<Integer, Long> sum : OnceFailingPartitionedSum.allSums.entrySet()) {
			assertEquals(new Long(sum.getKey() * NUM_STRINGS / NUM_KEYS), sum.getValue());
		}
		for (Long count : CounterSink.allCounts.values()) {
			assertEquals(new Long(NUM_STRINGS / NUM_KEYS), count);
		}

		assertEquals(NUM_KEYS, CounterSink.allCounts.size());
		assertEquals(NUM_KEYS, OnceFailingPartitionedSum.allSums.size());
	}

	// --------------------------------------------------------------------------------------------
	// Custom Functions
	// --------------------------------------------------------------------------------------------

	private static class IntGeneratingSourceFunction extends RichParallelSourceFunction<Integer>
			implements Checkpointed<Integer> {

		private final long numElements;

		private int index;
		private int step;

		private Random rnd = new Random();

		private volatile boolean isRunning = true;

		static final long[] counts = new long[PARALLELISM];

		@Override
		public void close() throws IOException {
			counts[getRuntimeContext().getIndexOfThisSubtask()] = index;
		}

		IntGeneratingSourceFunction(long numElements) {
			this.numElements = numElements;
		}

		@Override
		public void open(Configuration parameters) throws IOException {
			step = getRuntimeContext().getNumberOfParallelSubtasks();
			if (index == 0) {
				index = getRuntimeContext().getIndexOfThisSubtask();
			}
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			final Object lockingObject = ctx.getCheckpointLock();

			while (isRunning && index < numElements) {

				synchronized (lockingObject) {
					index += step;
					ctx.collect(index % NUM_KEYS);
				}

				if (rnd.nextDouble() < 0.025) {
					Thread.sleep(1);
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}

		@Override
		public Integer snapshotState(long checkpointId, long checkpointTimestamp) {
			return index;
		}

		@Override
		public void restoreState(Integer state) {
			index = state;
		}
	}

	private static class OnceFailingPartitionedSum extends RichMapFunction<Integer, Tuple2<Integer, Long>> {

		private static Map<Integer, Long> allSums = new ConcurrentHashMap<Integer, Long>();

		private static volatile boolean hasFailed = false;

		private final long numElements;

		private long failurePos;
		private long count;

		private OperatorState<Long> sum;

		OnceFailingPartitionedSum(long numElements) {
			this.numElements = numElements;
		}

		@Override
		public void open(Configuration parameters) throws IOException {
			long failurePosMin = (long) (0.6 * numElements / getRuntimeContext().getNumberOfParallelSubtasks());
			long failurePosMax = (long) (0.8 * numElements / getRuntimeContext().getNumberOfParallelSubtasks());

			failurePos = (new Random().nextLong() % (failurePosMax - failurePosMin)) + failurePosMin;
			count = 0;
			sum = getRuntimeContext().getKeyValueState("my_state", Long.class, 0L);
		}

		@Override
		public Tuple2<Integer, Long> map(Integer value) throws Exception {
			count++;
			if (!hasFailed && count >= failurePos) {
				hasFailed = true;
				throw new Exception("Test Failure");
			}

			long currentSum = sum.value() + value;
			sum.update(currentSum);
			allSums.put(value, currentSum);
			return new Tuple2<Integer, Long>(value, currentSum);
		}
	}

	private static class CounterSink extends RichSinkFunction<Tuple2<Integer, Long>> {

		private static Map<Integer, Long> allCounts = new ConcurrentHashMap<Integer, Long>();

		private OperatorState<NonSerializableLong> aCounts;
		private OperatorState<Long> bCounts;

		@Override
		public void open(Configuration parameters) throws IOException {
			aCounts = getRuntimeContext().getKeyValueState("a", NonSerializableLong.class, NonSerializableLong.of(0L));
			bCounts = getRuntimeContext().getKeyValueState("b", Long.class, 0L);
		}

		@Override
		public void invoke(Tuple2<Integer, Long> value) throws Exception {
			long ac = aCounts.value().value;
			long bc = bCounts.value();
			assertEquals(ac, bc);

			long currentCount = ac + 1;
			aCounts.update(NonSerializableLong.of(currentCount));
			bCounts.update(currentCount);

			allCounts.put(value.f0, currentCount);
		}
	}
}
