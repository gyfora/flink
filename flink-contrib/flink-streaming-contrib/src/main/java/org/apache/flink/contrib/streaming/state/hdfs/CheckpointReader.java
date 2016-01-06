package org.apache.flink.contrib.streaming.state.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.file.tfile.TFile.Reader;
import org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner;

public class CheckpointReader implements AutoCloseable {

	private Configuration conf;
	private FileSystem fs;
	private Reader reader;
	private Scanner scanner;

	public CheckpointReader(Path path) throws IOException {
		conf = new Configuration();
		fs = FileSystem.get(conf);
		reader = new Reader(fs.open(path), fs.getFileStatus(path).getLen(), conf);
		scanner = reader.createScanner();
	}

	public byte[] lookup(byte[] key) throws IOException {
		if (scanner.seekTo(key)) {
			int valueLen = scanner.entry().getValueLength();
			byte[] read = new byte[valueLen];
			scanner.entry().getValue(read);

			return read;
		} else {
			return null;
		}
	}

	@Override
	public void close() throws Exception {
		scanner.close();
		reader.close();
	}

}
