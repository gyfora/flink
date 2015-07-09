/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.junit.Test;

public class OutOfCoreStateTest {

	@Test
	public void test() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2);

		env.enableCheckpointing(5000);

		env.addSource(new RichParallelSourceFunction<Integer>() {

			private static final long serialVersionUID = 1L;
		
			volatile boolean isRunning = false;

			@Override
			public void run(
					org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext<Integer> ctx)
					throws Exception {
				isRunning = true;
				while (isRunning) {
					Thread.sleep(1000);
					ctx.collect(getRuntimeContext().getIndexOfThisSubtask());
				}
			}

			@Override
			public void cancel() {
				isRunning = false;
			}

		}).shuffle().addSink(new SinkFunction<Integer>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void invoke(Integer value) throws Exception {

			}
		});
		
		env.execute();
	}
}
