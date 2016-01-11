package org.apache.flink.contrib.streaming.state.hdfs;

import java.io.IOException;

public interface CheckpointMerger extends AutoCloseable {

	void merge() throws IOException;
}
