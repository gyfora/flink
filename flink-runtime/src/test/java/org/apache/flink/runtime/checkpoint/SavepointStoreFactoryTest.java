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
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsStateBackendFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SavepointStoreFactoryTest {

	@Test
	public void testStateStoreWithDefaultConfig() throws Exception {
		SavepointStore store = SavepointStoreFactory
				.createFromConfig(new Configuration());

		assertTrue(store.getStateStore() instanceof HeapStateStore);
	}

	@Test
	public void testSavepointSpecificConfig() throws Exception {
		Configuration config = new Configuration();
		config.setString(ConfigConstants.STATE_BACKEND, "filesystem");
		config.setString(SavepointStoreFactory.SAVEPOINT_BACKEND_KEY, "jobmanager");

		SavepointStore store = SavepointStoreFactory.createFromConfig(config);

		assertTrue(store.getStateStore() instanceof HeapStateStore);
	}

	@Test
	public void testStateStoreWithFileSystemBackend() throws Exception {
		Configuration config = new Configuration();

		String expectedRootPath = System.getProperty("java.io.tmpdir");

		config.setString(ConfigConstants.STATE_BACKEND, "filesystem");
		config.setString(SavepointStoreFactory.SAVEPOINT_DIRECTORY_KEY, expectedRootPath);

		SavepointStore store = SavepointStoreFactory.createFromConfig(config);

		assertTrue(store.getStateStore() instanceof FileSystemStateStore);

		FileSystemStateStore fsStore = (FileSystemStateStore) store.getStateStore();

		assertEquals(new Path(expectedRootPath), fsStore.getRootPath());
	}

	@Test
	public void testSavepointSpecificConfigWithFileSystemBackend() throws Exception {
		Configuration config = new Configuration();

		String expectedRootPath = System.getProperty("java.io.tmpdir");

		config.setString(ConfigConstants.STATE_BACKEND, "jobmanager");
		config.setString(SavepointStoreFactory.SAVEPOINT_BACKEND_KEY, "filesystem");
		config.setString(SavepointStoreFactory.SAVEPOINT_DIRECTORY_KEY, expectedRootPath);

		SavepointStore store = SavepointStoreFactory
				.createFromConfig(config);

		assertTrue(store.getStateStore() instanceof FileSystemStateStore);

		FileSystemStateStore fsStore = (FileSystemStateStore) store.getStateStore();

		assertEquals(new Path(expectedRootPath), fsStore.getRootPath());
	}

	@Test
	public void testStateStoreWithFileSystemBackendButNoDirectory() throws Exception {
		Configuration config = new Configuration();

		String expectedRootPath = System.getProperty("java.io.tmpdir");

		config.setString(ConfigConstants.STATE_BACKEND, "filesystem");
		config.setString(FsStateBackendFactory.CHECKPOINT_DIRECTORY_URI_CONF_KEY, expectedRootPath);

		SavepointStore store = SavepointStoreFactory
				.createFromConfig(config);

		assertTrue(store.getStateStore() instanceof FileSystemStateStore);

		FileSystemStateStore fsStore = (FileSystemStateStore) store.getStateStore();

		assertEquals(new Path(expectedRootPath), fsStore.getRootPath());
	}

	@Test
	public void testStateStoreWithFileSystemButNoDirectoryFallbackToJobManager() throws Exception {
		Configuration config = new Configuration();
		config.setString(ConfigConstants.STATE_BACKEND, "filesystem");

		SavepointStore store = SavepointStoreFactory
				.createFromConfig(config);

		assertTrue(store.getStateStore() instanceof HeapStateStore);
	}
}
