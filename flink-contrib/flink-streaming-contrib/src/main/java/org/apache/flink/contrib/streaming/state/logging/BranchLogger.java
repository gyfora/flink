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

public class BranchLogger implements Serializable {
	private static final long serialVersionUID = 1L;

	private final Logger LOG;
	private final int frequency;
	private final String message;

	private long totalCount = 0;
	private long hitCount = 0;

	private final boolean debug;

	public BranchLogger(int frequency, String message, Logger LOG) {
		this.frequency = frequency;
		this.LOG = LOG;
		this.message = "Branch log (" + message + ") (totalCount, hitCount) = ({}, {})";
		this.debug = LOG.isDebugEnabled();
	}

	public void start() {
		if (debug) {
			totalCount++;
			if (totalCount - 1 > 0 && (totalCount - 1) % frequency == 0) {
				LOG.debug(message, totalCount - 1, ((double) hitCount) / (totalCount - 1));
				totalCount = 1;
				hitCount = 0;
			}
		}
	}

	public void hit() {
		if (debug) {
			hitCount++;

		}
	}

}
