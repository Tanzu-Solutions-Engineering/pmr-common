package com.gopivotal.mapred.input;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

import com.gopivotal.mapred.input.WholeFileInputFormat.WholeFileRecordReader;

/**
 * An extension of {@link WholeFileInputFormat} that will combine files together
 * to make larger input splits.
 * 
 * Use {@link CombineWholeFileInputFormat#setMaxSplitSize(long)} to set the file
 * size, in bytes, that is ideal.
 * {@link PathUtil#getIdealSplitSize(FileSystem, Path[], int)} may be helpful.
 */
public class CombineWholeFileInputFormat extends
		CombineFileInputFormat<Text, BytesWritable> {

	@Override
	public RecordReader<Text, BytesWritable> getRecordReader(InputSplit split,
			JobConf conf, Reporter reporter) throws IOException {
		return new CombineWholeFileRecordReader(split, conf);
	}

	public static class CombineWholeFileRecordReader implements
			RecordReader<Text, BytesWritable> {

		private WholeFileRecordReader rdr = null;
		private CombineFileSplit split = null;
		private int currentSplit = 0;
		private JobConf conf = null;

		public CombineWholeFileRecordReader(InputSplit split, JobConf conf)
				throws IOException {
			this.conf = conf;
			split = (CombineFileSplit) split;

			if (split.getLength() != 0) {
				initializeNextReader();
			}
		}

		@Override
		public boolean next(Text key, BytesWritable value) throws IOException {

			do {
				if (rdr.next(key, value)) {
					return true;
				} else if (currentSplit < split.getNumPaths()) {
					initializeNextReader();
				} else {
					return false;
				}

			} while (true);
		}

		@Override
		public void close() throws IOException {
			rdr.close();
		}

		@Override
		public float getProgress() throws IOException {
			return (currentSplit - 1 + rdr.getProgress()) / split.getNumPaths();
		}

		private void initializeNextReader() throws IOException {

			rdr = new WholeFileRecordReader(new FileSplit(
					split.getPath(currentSplit), split.getOffset(currentSplit),
					split.getLength(currentSplit), new String[] {}), conf);

			++currentSplit;
		}

		@Override
		public Text createKey() {
			return rdr.createKey();
		}

		@Override
		public BytesWritable createValue() {
			return rdr.createValue();
		}

		@Override
		public long getPos() throws IOException {
			return 0;
		}
	}
}
