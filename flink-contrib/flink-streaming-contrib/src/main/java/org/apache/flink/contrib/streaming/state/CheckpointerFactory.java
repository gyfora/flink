package org.apache.flink.contrib.streaming.state;

import java.io.IOException;
import java.io.Serializable;

import org.apache.flink.contrib.streaming.state.hdfs.CheckpointReader;
import org.apache.flink.contrib.streaming.state.hdfs.CheckpointWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public interface CheckpointerFactory extends Serializable {

	CheckpointReader createReader(FileSystem fs, Path path, KvStateConfig conf) throws IOException;

	CheckpointWriter createWriter(FileSystem fs, Path path, KvStateConfig conf) throws IOException;
}
