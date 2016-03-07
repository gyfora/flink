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

package org.apache.flink.streaming.runtime.tasks;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecordSerializer;
import org.apache.flink.types.Either;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
public class StreamIterationHead<IN> extends OneInputStreamTask<IN, IN> {

	private static final Logger LOG = LoggerFactory.getLogger(StreamIterationHead.class);

	private volatile boolean running = true;

	/**
	 * A flag that is on during the duration of a checkpoint. While onSnapshot
	 * is true the iteration head has to perform upstream backup of all records
	 * in transit within the loop.
	 */
	private volatile boolean onSnapshot = false;

	/**
	 * Flag notifying whether the iteration head has flushed pending
	 */
	private boolean hasFlushed = false;

	private volatile RecordWriterOutput<IN>[] outputs;

	private UpstreamLogger upstreamLogger;

	private Object lock;

	private String brokerID;

	@Override
	public void init() throws Exception {
		this.lock = getCheckpointLock();
		this.upstreamLogger = new UpstreamLogger();
		this.headOperator = upstreamLogger;
		operatorChain = new OperatorChain<>(this, headOperator,
				getEnvironment().getAccumulatorRegistry().getReadWriteReporter());
		headOperator.setup(this, getConfiguration(), getHeadOutput());
	}

	@Override
	protected void run() throws Exception {

		final String iterationId = getConfiguration().getIterationId();
		if (iterationId == null || iterationId.length() == 0) {
			throw new Exception("Missing iteration ID in the task configuration");
		}
		brokerID = createBrokerIdString(getEnvironment().getJobID(), iterationId,
				getEnvironment().getTaskInfo().getIndexOfThisSubtask());
		final long iterationWaitTime = getConfiguration().getIterationWaitTime();
		final boolean shouldWait = iterationWaitTime > 0;

		final BackChannel<Either<StreamRecord<IN>, CheckpointBarrier>> dataChannel = createBackChannel();

		// offer the queue for the tail
		BackChannelBroker.INSTANCE.handIn(brokerID, dataChannel);
		LOG.info("Iteration head {} added feedback queue under {}", getName(), brokerID);

		// do the work
		try {
			outputs = (RecordWriterOutput<IN>[]) getStreamOutputs();

			// If timestamps are enabled we make sure to remove cyclic watermark
			// dependencies
			if (isSerializingTimestamps()) {
				for (RecordWriterOutput<IN> output : outputs) {
					output.emitWatermark(new Watermark(Long.MAX_VALUE));
				}
			}

			// emit in-flight events in the upstream log upon initialization
			synchronized (lock) {
				LOG.debug("Initializing iteration by flushing the upstream backup if not empty at {}", brokerID);
				upstreamLogger.flushAndClearLog();
				hasFlushed = true;
			}
			while (running) {
				Either<StreamRecord<IN>, CheckpointBarrier> nextRecord = shouldWait
						? dataChannel.receive(iterationWaitTime, TimeUnit.MILLISECONDS) : dataChannel.receive();

				synchronized (lock) {

					if (nextRecord != null) {

						if (nextRecord.isLeft()) {
							if (onSnapshot) {
								upstreamLogger.processElement(nextRecord.left());
							} else {
								for (RecordWriterOutput<IN> output : outputs) {
									output.collect(nextRecord.left());
								}
							}
						} else {
							LOG.debug("Received barrier on backedge for {}, takeing snapshot and flushing log...",
									brokerID);
							checkpointStatesInternal(nextRecord.right().getId(), nextRecord.right().getTimestamp());
							upstreamLogger.flushAndClearLog();
							onSnapshot = false;
						}

					} else {
						LOG.debug("Input stream ended for {}, flushing upstream log if not empty...", brokerID);
						upstreamLogger.flushAndClearLog();
						break;
					}
				}
			}
		} finally {
			// make sure that we remove the queue from the broker, to prevent a
			// resource leak
			BackChannelBroker.INSTANCE.remove(brokerID);
			LOG.info("Iteration head {} removed feedback queue under {}", getName(), brokerID);
		}
	}

	private BackChannel<Either<StreamRecord<IN>, CheckpointBarrier>> createBackChannel() {
		return new BackChannel<Either<StreamRecord<IN>,CheckpointBarrier>>() {
			
			BlockingQueue<Either<StreamRecord<IN>,CheckpointBarrier>> q = new ArrayBlockingQueue<>(100);

			@Override
			public void send(Either<StreamRecord<IN>, CheckpointBarrier> data) throws Exception {
				q.put(data);
			}

			@Override
			public Either<StreamRecord<IN>, CheckpointBarrier> receive() throws Exception {
				return q.take();
			}

			@Override
			public Either<StreamRecord<IN>, CheckpointBarrier> receive(long timeout, TimeUnit timeUnit) throws Exception {
				return q.poll(timeout, timeUnit);
			}
		};
	}

	@Override
	protected void cancelTask() {
		running = false;
	}

	@Override
	protected void cleanup() throws Exception {
		// nothing to cleanup
	}

	// ------------------------------------------------------------------------
	// Utilities
	// ------------------------------------------------------------------------

	/**
	 * Creates the identification string with which head and tail task find the
	 * shared blocking queue for the back channel. The identification string is
	 * unique per parallel head/tail pair per iteration per job.
	 *
	 * @param jid
	 *            The job ID.
	 * @param iterationID
	 *            The id of the iteration in the job.
	 * @param subtaskIndex
	 *            The parallel subtask number
	 * @return The identification string.
	 */
	public static String createBrokerIdString(JobID jid, String iterationID, int subtaskIndex) {
		return jid + "-" + iterationID + "-" + subtaskIndex;
	}

	@Override
	public boolean triggerCheckpoint(long checkpointId, long timestamp) throws Exception {

		// invoked upon barrier from Runtime
		synchronized (lock) {
			operatorChain.broadcastCheckpointBarrier(checkpointId, timestamp);

			if (onSnapshot || !hasFlushed) {
				LOG.debug("Iteration head {} aborting checkpoint {}", getName(), checkpointId);
				return false;
			}
			LOG.debug("Received trigger checkpoint message at {}, starting upstream log...", brokerID);
			onSnapshot = true;
			return true;
		}
	}

	/**
	 * Internal operator that solely serves as a state logging facility for
	 * persisting and restoring upstream backups
	 */
	public class UpstreamLogger extends AbstractStreamOperator<IN>
			implements OneInputStreamOperator<IN, IN> {

		private static final long serialVersionUID = 1L;

		/**
		 * The upstreamLog is used to store all records that should be logged
		 * throughout the duration of each checkpoint instance. These are part
		 * of the iteration head operator state for that snapshot and represent
		 * the records in transit for the backedge of an iteration cycle.
		 */
		private ListState<StreamRecord<IN>> listState;
		private ListStateDescriptor<StreamRecord<IN>> stateDescriptor;

		@Override
		public void open() throws Exception {
			super.open();
			this.listState = this.getStateBackend().getPartitionedState(null, VoidSerializer.INSTANCE, stateDescriptor);
			this.getStateBackend().setCurrentKey(true);
		}

		@Override
		public void setup(StreamTask<?, ?> task, StreamConfig config, Output<StreamRecord<IN>> output) {
			config.setStateKeySerializer(BooleanSerializer.INSTANCE);
			super.setup(task, config, output);

			stateDescriptor = new ListStateDescriptor<>(
					"upstream-log",
					new StreamRecordSerializer<>(
							task.getConfiguration().<IN> getTypeSerializerOut(task.getUserCodeClassLoader())));
		}

		@Override
		public void processElement(StreamRecord<IN> element) throws Exception {
			listState.add(element);
		}

		@Override
		public void processWatermark(Watermark mark) throws Exception {
			throw new RuntimeException("Watermarks should not be forwarded on the back edges.");
		}

		public void flushAndClearLog() throws Exception {
			long count = 0;
			for (StreamRecord<IN> record : listState.get()) {
				output.collect(record);
				count++;
			}
			listState.clear();
			if (count > 0) {
				LOG.debug("Flushed {} records at {}", count, brokerID);
			} else {
				LOG.debug("Upstream backup was empty at {}", brokerID);
			}
		}

	}

}
