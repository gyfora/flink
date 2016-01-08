package org.apache.flink.contrib.streaming.state.hdfs;

import org.apache.flink.contrib.streaming.state.CheckpointerFactory;
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
