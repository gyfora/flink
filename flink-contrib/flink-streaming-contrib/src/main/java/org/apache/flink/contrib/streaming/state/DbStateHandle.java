/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import static org.apache.flink.contrib.streaming.state.SQLRetrier.retry;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.util.concurrent.Callable;

import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.util.InstantiationUtil;
import org.eclipse.jetty.util.log.Log;

/**
 * State handle implementation for storing checkpoints as byte arrays in
 * databases using the {@link DbAdapter} defined in the {@link DbBackendConfig}.
 * 
 */
public class DbStateHandle<S> implements Serializable, StateHandle<S> {

	private static final long serialVersionUID = 1L;
	private static final int NUM_RETRIES = 5;

	private final String jobId;
	private final DbBackendConfig dbConfig;

	private final long checkpointId;
	private final long checkpointTs;

	private final long handleId;

	public DbStateHandle(String jobId, long checkpointId, long checkpointTs, long handleId, DbBackendConfig dbConfig) {
		this.checkpointId = checkpointId;
		this.handleId = handleId;
		this.jobId = jobId;
		this.dbConfig = dbConfig;
		this.checkpointTs = checkpointTs;
	}

	protected byte[] getBytes() throws IOException {
		return retry(new Callable<byte[]>() {
			public byte[] call() throws Exception {
				try (Connection con = dbConfig.createConnection()) {
					return dbConfig.getDbAdapter().getCheckpoint(jobId, con, checkpointId, checkpointTs, handleId);
				}
			}
		}, NUM_RETRIES);
	}

	@Override
	public void discardState() {
		try {
			retry(new Callable<Boolean>() {
				public Boolean call() throws Exception {
					try (Connection con = dbConfig.createConnection()) {
						dbConfig.getDbAdapter().deleteCheckpoint(jobId, con, checkpointId, checkpointTs, handleId);
					}
					return true;
				}
			}, NUM_RETRIES);
		} catch (IOException e) {
			// We don't want to fail the job here, but log the error.
			if (Log.isDebugEnabled()) {
				Log.debug("Could not discard state.");
			}
		}
	}

	@Override
	public S getState(ClassLoader userCodeClassLoader) throws IOException, ClassNotFoundException {
		return InstantiationUtil.deserializeObject(getBytes(), userCodeClassLoader);
	}
}