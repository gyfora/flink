package org.apache.flink.contrib.streaming.state.hdfs;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;

public class KeyScanner implements AutoCloseable {
	private final LinkedList<Path> paths;
	private final Map<Path, CheckpointReader> openReaders = new HashMap<>();

	public KeyScanner(List<Path> paths) {
		this.paths = new LinkedList<>(paths);
	}

	public KeyScanner() {
		this(new LinkedList<Path>());
	}

	public void addNewLookupFile(Path path) {
		paths.addFirst(path);
	}

	public byte[] lookup(byte[] key) throws IOException {
		for (Path checkpointPath : paths) {
			CheckpointReader reader = openReaders.get(checkpointPath);
			if (reader == null) {
				reader = new CheckpointReader(checkpointPath);
				openReaders.put(checkpointPath, reader);
			}
			byte[] value = reader.lookup(key);
			if (value != null) {
				return value;
			}
		}
		return null;
	}

	@Override
	public void close() throws Exception {
		for (CheckpointReader r : openReaders.values()) {
			r.close();
		}
	}

	@Override
	public String toString() {
		return "KeyScanner: " + paths;
	}

}
