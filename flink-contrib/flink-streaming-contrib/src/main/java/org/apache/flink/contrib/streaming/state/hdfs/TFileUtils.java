package org.apache.flink.contrib.streaming.state.hdfs;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.file.tfile.TFile.Reader;
import org.apache.hadoop.io.file.tfile.TFile.Writer;
import org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner;

public class TFileUtils {

	public static void compact(Path first, Path second, Path newFile) throws IOException {

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Reader readerFirst = new Reader(fs.open(first), fs.getFileStatus(first).getLen(), conf);
		Scanner scannerFirst = readerFirst.createScanner();

		Reader readerSecond = new Reader(fs.open(second), fs.getFileStatus(second).getLen(), conf);
		Scanner scannerSecond = readerSecond.createScanner();

		FSDataOutputStream fout = fs.create(newFile);
		Writer writer = new Writer(fout, CheckpointWriter.minBlockSize, "none", "memcmp", conf);

		Comparator<Scanner.Entry> entryComparator = readerFirst.getEntryComparator();

		while (!scannerFirst.atEnd() && !scannerSecond.atEnd()) {

			Scanner.Entry e1 = scannerFirst.entry();
			Scanner.Entry e2 = scannerSecond.entry();

			int c = entryComparator.compare(e1, e2);

			if (c < 0) {
				copyEntry(e1, writer);
				scannerFirst.advance();
			} else if (c > 0) {
				copyEntry(e2, writer);
				scannerSecond.advance();
			} else {
				copyEntry(e2, writer);
				scannerFirst.advance();
				scannerSecond.advance();
			}
		}

		Scanner nonEmpty = !scannerFirst.atEnd() ? scannerFirst : scannerSecond;

		while (!nonEmpty.atEnd()) {
			Scanner.Entry e = nonEmpty.entry();
			copyEntry(e, writer);
			nonEmpty.advance();
		}

		scannerFirst.close();
		scannerSecond.close();
		readerFirst.close();
		readerSecond.close();
		fout.close();
		writer.close();
	}

	public static void copyEntry(Scanner.Entry from, Writer to) throws IOException {
		DataOutputStream ko = to.prepareAppendKey(from.getKeyLength());
		from.writeKey(ko);
		ko.close();
		DataOutputStream vo = to.prepareAppendValue(from.getValueLength());
		from.writeValue(vo);
		vo.close();
	}
}
