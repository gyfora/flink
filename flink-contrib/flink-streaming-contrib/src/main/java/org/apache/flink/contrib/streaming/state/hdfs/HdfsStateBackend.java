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
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.runtime.state.ArrayListSerializer;
import org.apache.flink.runtime.state.GenericListState;
import org.apache.flink.runtime.state.GenericReducingState;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsStateBackend extends FsStateBackend {

	private static final Logger LOG = LoggerFactory.getLogger(HdfsStateBackend.class);
	private static final long serialVersionUID = 1L;

	private org.apache.flink.core.fs.Path localDir;

	private transient Path checkpointPath;
	private transient FileSystem hadoopFs;
	private transient Path localPath;
	private transient FileSystem localFs;

	private transient Environment env;
	private HdfsKvStateConfig kvStateConf;
	private String operatorIdentifier;

	public HdfsStateBackend(String checkpointDataUri, String localDataUri, HdfsKvStateConfig kvStateConf)
			throws IOException {
		super(checkpointDataUri);
		this.kvStateConf = kvStateConf;
		this.localDir = new org.apache.flink.core.fs.Path(localDataUri);
	}

	public HdfsStateBackend(org.apache.flink.core.fs.Path checkpointDataUri, org.apache.flink.core.fs.Path localDataUri,
			HdfsKvStateConfig kvStateConf)
					throws IOException {
		super(checkpointDataUri);
		this.kvStateConf = kvStateConf;
		this.localDir = localDataUri;
	}

	@Override
	public void initializeForJob(Environment env,
			String operatorIdentifier,
			TypeSerializer<?> keySerializer) throws Exception {
		super.initializeForJob(env, operatorIdentifier, keySerializer);
		this.operatorIdentifier = operatorIdentifier;

		checkpointPath = new Path(new Path(getCheckpointDirectory().toUri()), "kvstate");
		localPath = new Path(new Path(localDir.toUri()), env.getApplicationID().toString());
		localPath = new Path(localPath, "kvstate-local");

		if (getFileSystem() instanceof HadoopFileSystem) {
			hadoopFs = ((HadoopFileSystem) getFileSystem()).getHadoopFileSystem();
		} else {
			hadoopFs = checkpointPath.getFileSystem(HadoopFileSystem.getHadoopConfiguration());
		}

		localFs = localPath.getFileSystem(new Configuration());

		hadoopFs.mkdirs(checkpointPath);
		localFs.mkdirs(localPath);

		LOG.debug("Checkpoint directories created: {}, {}", checkpointPath, localPath);

		this.env = env;
	}

	@Override
	public <N, T> ValueState<T> createValueState(TypeSerializer<N> namespaceSerializer,
			ValueStateDescriptor<T> stateDesc) throws Exception {

		if (!stateDesc.isSerializerInitialized()) {
			throw new IllegalArgumentException("state descriptor serializer not initialized");
		}

		String stateId = operatorIdentifier + "_" + stateDesc.getName();

		Path cpPath = new Path(new Path(checkpointPath, String.valueOf(env.getTaskInfo().getIndexOfThisSubtask())),
				stateId);
		Path tmpPath = new Path(new Path(localPath, String.valueOf(env.getTaskInfo().getIndexOfThisSubtask())),
				stateId);
		hadoopFs.mkdirs(cpPath);
		localFs.mkdirs(tmpPath);

		return new HdfsKvState<>(this, stateId, kvStateConf, keySerializer, namespaceSerializer, stateDesc,
				0, 1, hadoopFs, cpPath, localFs, tmpPath, new HashMap<Interval, URI>());
	}

	@Override
	public <N, T> ListState<T> createListState(TypeSerializer<N> namespaceSerializer,
			ListStateDescriptor<T> stateDesc) throws Exception {

		if (!stateDesc.isSerializerInitialized()) {
			throw new IllegalArgumentException("state descriptor serializer not initialized");
		}

		ValueStateDescriptor<ArrayList<T>> valueStateDescriptor = new ValueStateDescriptor<>(stateDesc.getName(),
				new ArrayListSerializer<>(stateDesc.getSerializer()), null);

		ValueState<ArrayList<T>> valueState = createValueState(namespaceSerializer, valueStateDescriptor);
		return new GenericListState<>(valueState);
	}

	@Override
	public <N, T> ReducingState<T> createReducingState(TypeSerializer<N> namespaceSerializer,
			ReducingStateDescriptor<T> stateDesc) throws Exception {

		if (!stateDesc.isSerializerInitialized()) {
			throw new IllegalArgumentException("state descriptor serializer not initialized");
		}

		ValueStateDescriptor<T> valueStateDescriptor = new ValueStateDescriptor<>(
				stateDesc.getName(), stateDesc.getSerializer(), null);

		ValueState<T> valueState = createValueState(namespaceSerializer, valueStateDescriptor);
		return new GenericReducingState<>(valueState, stateDesc.getReduceFunction());
	}

	public FileSystem getHadoopFileSystem() {
		return hadoopFs;
	}

	public HdfsKvStateConfig getKvStateConf() {
		return kvStateConf;
	}

	public FileSystem getLocalFileSystem() {
		return localFs;
	}
}
