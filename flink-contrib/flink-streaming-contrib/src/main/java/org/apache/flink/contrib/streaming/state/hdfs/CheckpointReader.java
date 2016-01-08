package org.apache.flink.contrib.streaming.state.hdfs;

import java.io.IOException;
import java.io.Serializable;

public interface CheckpointReader extends AutoCloseable, Serializable {

	public byte[] lookup(byte[] key) throws IOException;

}
