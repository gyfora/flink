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

import org.apache.flink.contrib.streaming.state.KvStateConfig;

public class HdfsKvStateConfig extends KvStateConfig {

	private static final long serialVersionUID = 1L;
	private CheckpointerFactory checkpointerFactory;

	public HdfsKvStateConfig(int kvStateCacheSize, double maxCacheEvictFraction) {
		this(kvStateCacheSize, maxCacheEvictFraction, new TFileCheckpointerFactory());
	}

	public HdfsKvStateConfig(int kvStateCacheSize, double maxCacheEvictFraction,
			CheckpointerFactory checkpointerFactory) {
		super(kvStateCacheSize, maxCacheEvictFraction);
		this.checkpointerFactory = checkpointerFactory;
	}

	public CheckpointerFactory getCheckpointerFactory() {
		return checkpointerFactory;
	}

	public void setCheckpointerFactory(CheckpointerFactory checkpointerFactory) {
		this.checkpointerFactory = checkpointerFactory;
	}

}
