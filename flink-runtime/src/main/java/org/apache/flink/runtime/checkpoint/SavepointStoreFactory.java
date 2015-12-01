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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackendFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for savepoint {@link StateStore} instances.
 */
public class SavepointStoreFactory {

	public static final String SAVEPOINT_BACKEND_KEY = "state.backend.savepoints";
	public static final String SAVEPOINT_DIRECTORY_KEY = "state.backend.savepoints.fs.dir";

	public static final Logger LOG = LoggerFactory.getLogger(SavepointStoreFactory.class);

	public static SavepointStore createFromConfig(
			Configuration config) throws Exception {

		String backendName = config.getString(SAVEPOINT_BACKEND_KEY, null);

		if (backendName == null) {
			LOG.warn("No specific savepoint state backend has been configured. Using configured " +
					"state backend.");

			backendName = config.getString(ConfigConstants.STATE_BACKEND, null);

			if (backendName == null) {
				LOG.warn("No state backend has been specified. Using default state " +
						"backend (JobManager)");
				backendName = "jobmanager";
			}
		}

		backendName = backendName.toLowerCase();
		switch (backendName) {
			case "jobmanager":
				LOG.info("Savepoint state backend is set to JobManager (heap memory).");
				return new SavepointStore(new HeapStateStore<Savepoint>());

			case "filesystem":
				String rootPath = config.getString(SAVEPOINT_DIRECTORY_KEY, null);

				if (rootPath == null) {
					String checkpointPath = config.getString(
							FsStateBackendFactory.CHECKPOINT_DIRECTORY_URI_CONF_KEY, null);

					LOG.warn("Using filesystem as state backend "
							+ "for savepoints, but did not specify directory. Please set the "
							+ "following configuration key: '"
							+ SAVEPOINT_DIRECTORY_KEY + "' (e.g. "
							+ SAVEPOINT_DIRECTORY_KEY + ": hdfs:///flink/savepoints/). " +
							"Falling back to value of '" + FsStateBackendFactory
							.CHECKPOINT_DIRECTORY_URI_CONF_KEY + "': " + checkpointPath + ".");

					if (checkpointPath == null) {
						LOG.warn("FileSystem savepoint backend is not configured. " +
								"Please set the following configuration key: '"
								+ SAVEPOINT_DIRECTORY_KEY + "' (e.g. "
								+ SAVEPOINT_DIRECTORY_KEY + ": hdfs:///flink/" +
								"savepoints/). Falling back to job manager backend.");

						return new SavepointStore(new HeapStateStore<Savepoint>());
					}
					else {
						// Set fall back
						rootPath = checkpointPath;
					}
				}

				LOG.info("Savepoint state backend is set to FileSystem (" + rootPath + ")");

				return new SavepointStore(new FileSystemStateStore<Savepoint>(
						rootPath, "savepoint-"));

			default:
				LOG.warn("Unrecognized state backend '" + backendName + "' for savepoints. " +
						"Falling back to job manager backend.");

				return new SavepointStore(new HeapStateStore<Savepoint>());
		}
	}

}
