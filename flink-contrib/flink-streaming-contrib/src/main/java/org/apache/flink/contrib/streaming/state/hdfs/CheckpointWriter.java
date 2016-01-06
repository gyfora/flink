package org.apache.flink.contrib.streaming.state.hdfs;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.file.tfile.TFile.Writer;

import com.google.common.primitives.UnsignedBytes;

public class CheckpointWriter implements AutoCloseable {

	public static final int minBlockSize = 512;

	private Configuration conf;
	private FileSystem fs;
	private FSDataOutputStream fout;
	private Writer writer;

	public CheckpointWriter(Path path) throws IOException {
		conf = new Configuration();
		fs = FileSystem.get(conf);
		fout = fs.create(path);
		writer = new Writer(fout, minBlockSize, "none", "memcmp", conf);
	}

	public void writeSorted(SortedMap<byte[], byte[]> kvPairs) throws IOException {
		for (Entry<byte[], byte[]> kv : kvPairs.entrySet()) {
			writer.append(kv.getKey(), kv.getValue());
		}
	}

	public void writeUnsorted(Map<byte[], byte[]> kvPairs) throws IOException {
		TreeMap<byte[], byte[]> sorted = new TreeMap<>(UnsignedBytes.lexicographicalComparator());
		sorted.putAll(kvPairs);
		writeSorted(sorted);
	}

	@Override
	public void close() throws Exception {
		writer.close();
		fout.close();
	}

}
