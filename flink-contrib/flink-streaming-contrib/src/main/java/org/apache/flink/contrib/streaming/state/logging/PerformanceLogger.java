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

package org.apache.flink.contrib.streaming.state.logging;

import java.io.Serializable;

import org.slf4j.Logger;

public class PerformanceLogger implements Serializable {
	private static final long serialVersionUID = 1L;

	private final Logger LOG;
	private final int frequency;
	private final String message;

	private long startTime = -1;
	private long totalTime = 0;
	private int count = 0;

	public PerformanceLogger(int frequency, String message, Logger LOG) {
		this.frequency = frequency;
		this.LOG = LOG;
		this.message = "Performance log (" + message + ") (numOperations, totalTime) = ({}, {})";
	}

	public void startMeasurement() {
		if (startTime > 0) {
			throw new RuntimeException("Started twice in a row");
		}
		startTime = System.nanoTime();
	}

	public void stopMeasurement() {
		if (startTime < 0) {
			throw new RuntimeException("Start first");
		}
		count++;
		totalTime += (System.nanoTime() - startTime);
		startTime = -1;

		if (count > 0 && count % frequency == 0) {
			LOG.info(message, frequency, totalTime / 1000000);
			totalTime = 0;
			count = 0;
		}
	}
}
