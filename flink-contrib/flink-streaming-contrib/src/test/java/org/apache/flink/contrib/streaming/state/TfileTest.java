package org.apache.flink.contrib.streaming.state;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.contrib.streaming.state.hdfs.CheckpointWriter;
import org.apache.flink.contrib.streaming.state.hdfs.KeyScanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import com.google.common.collect.Lists;

public class TfileTest {

	private static void testBasic() throws IOException {
		Path cpFile1 = new Path("/Users/gyulafora/Test", "basic.tfile1");
		Path cpFile2 = new Path("/Users/gyulafora/Test", "basic.tfile2");
		Path cpFile3 = new Path("/Users/gyulafora/Test", "basic.tfile3");

		Map<byte[], byte[]> kvs = new HashMap<>();

		kvs.put(new byte[2], new byte[4]);
		kvs.put(new byte[3], new byte[3]);
		kvs.put(new byte[6], new byte[5]);

		try (CheckpointWriter w = new CheckpointWriter(cpFile1)) {
			w.writeUnsorted(kvs);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		kvs.clear();

		kvs.put(new byte[1], new byte[1]);
		kvs.put(new byte[2], new byte[6]);
		kvs.put(new byte[4], new byte[1]);

		try (CheckpointWriter w = new CheckpointWriter(cpFile2)) {
			w.writeUnsorted(kvs);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		kvs.clear();

		kvs.put(new byte[1], new byte[4]);
		kvs.put(new byte[8], new byte[8]);

		try (CheckpointWriter w = new CheckpointWriter(cpFile3)) {
			w.writeUnsorted(kvs);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		try (KeyScanner r = new KeyScanner(Lists.newArrayList(cpFile3, cpFile2, cpFile1))) {
			System.out.println(Arrays.toString(r.lookup(new byte[8])));
			System.out.println(Arrays.toString(r.lookup(new byte[1])));
			System.out.println(Arrays.toString(r.lookup(new byte[2])));
			System.out.println(Arrays.toString(r.lookup(new byte[6])));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		FileSystem.get(new Configuration()).delete(cpFile1);
		FileSystem.get(new Configuration()).delete(cpFile2);
		FileSystem.get(new Configuration()).delete(cpFile1);
	}

	private static void testFileCreation() throws IOException {
		Path path = new Path("/Users/gyulafora/Test");
		FileSystem fs = FileSystem.get(new Configuration());

		RemoteIterator<LocatedFileStatus> it = fs.listFiles(path, false);

		while (it.hasNext()) {
			Path p = it.next().getPath();
			System.out.println(p.getName());
		}
	}

	public static void main(String[] args) throws IOException {
		// testBasic();
		// testFileCreation();

	}
}
