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

package org.apache.flink.contrib.streaming.state.hdfs;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.contrib.streaming.state.KvStateConfig;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TFileStateBackend extends FsStateBackend {

	private static final long serialVersionUID = 1L;

	private transient Path checkpointDir;
	private transient FileSystem fs;
	private transient Environment env;
	private KvStateConfig kvStateConf;

	public TFileStateBackend(String checkpointDataUri, KvStateConfig kvStateConf) throws IOException {
		super(checkpointDataUri);
		this.kvStateConf = kvStateConf;
	}

	public TFileStateBackend(org.apache.flink.core.fs.Path checkpointDataUri, KvStateConfig kvStateConf)
			throws IOException {
		super(checkpointDataUri);
		this.kvStateConf = kvStateConf;
	}

	@Override
	public <K, V> KvState<K, V, FsStateBackend> createKvState(String stateId, String stateName,
			TypeSerializer<K> keySerializer, TypeSerializer<V> valueSerializer, V defaultValue) throws Exception {
		Path p = new Path(new Path(checkpointDir, String.valueOf(env.getTaskInfo().getIndexOfThisSubtask())), stateId);
		fs.mkdirs(p);
		return new TFileKvState<K, V>(kvStateConf, fs, p, new ArrayList<Path>(), keySerializer, valueSerializer,
				defaultValue, 0);
	}

	@Override
	public void initializeForJob(Environment env) throws Exception {
		super.initializeForJob(env);

		checkpointDir = new Path(new Path(getCheckpointDirectory().toUri()), "kvstate");
		try {
			fs = checkpointDir.getFileSystem(new Configuration());
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		this.env = env;
	}

	public FileSystem getHadoopFileSystem() {
		return fs;
	}

	public KvStateConfig getKvStateConf() {
		return kvStateConf;
	}
}
