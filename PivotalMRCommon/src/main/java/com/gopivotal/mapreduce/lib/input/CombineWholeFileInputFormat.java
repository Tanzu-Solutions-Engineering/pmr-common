package com.gopivotal.mapreduce.lib.input;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import com.gopivotal.mapreduce.lib.input.WholeFileInputFormat.WholeFileRecordReader;

/**
 * An extension of WholeFileInputFormat that will combine files together to make
 * larger input splits.
 * 
 * Use {@link CombineWholeFileInputFormat#setMaxSplitSize(long)} to set the file
 * size, in bytes, that is ideal.
 */
public class CombineWholeFileInputFormat extends
		CombineFileInputFormat<Text, BytesWritable> {

	@Override
	public RecordReader<Text, BytesWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		return new CombineWholeFileRecordReader();
	}

	public static class CombineWholeFileRecordReader extends
			RecordReader<Text, BytesWritable> {

		private WholeFileRecordReader rdr = null;
		private CombineFileSplit split = null;
		private int currentSplit = 0;
		private TaskAttemptContext context = null;

		@Override
		public void initialize(InputSplit paramInputSplit,
				TaskAttemptContext paramTaskAttemptContext) throws IOException,
				InterruptedException {
			context = paramTaskAttemptContext;
			split = (CombineFileSplit) paramInputSplit;

			if (split.getLength() != 0) {
				initializeNextReader();
			}
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {

			do {
				if (rdr.nextKeyValue()) {
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
		public Text getCurrentKey() throws IOException, InterruptedException {
			return rdr.getCurrentKey();
		}

		@Override
		public BytesWritable getCurrentValue() throws IOException,
				InterruptedException {
			return rdr.getCurrentValue();
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return (currentSplit - 1 + rdr.getProgress()) / split.getNumPaths();
		}

		private void initializeNextReader() throws IOException,
				InterruptedException {

			rdr = new WholeFileRecordReader();
			rdr.initialize(
					new FileSplit(split.getPath(currentSplit), split
							.getOffset(currentSplit), split
							.getLength(currentSplit), null), context);

			++currentSplit;
		}
	}
}
