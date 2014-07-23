package com.gopivotal.mapreduce.lib.input;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * A {@link FileInputFormat} implementation that passes the file name as the key
 * and the bytes of the file as the value. Generates one map task per file, but
 * the {@link CombineWholeFileInputFormat} could be used to batch them together
 * into a configurable number of map tasks.
 */
public class WholeFileInputFormat extends FileInputFormat<Text, BytesWritable> {

	@Override
	public boolean isSplitable(JobContext context, Path p) {
		return false;
	}

	@Override
	public RecordReader<Text, BytesWritable> createRecordReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException,
			InterruptedException {
		return new WholeFileRecordReader();
	}

	public static class WholeFileRecordReader extends
			RecordReader<Text, BytesWritable> {

		private Text key = new Text();
		private BytesWritable value = new BytesWritable();
		private boolean read = false;
		private FileSystem fs = null;
		private FileSplit fSplit = null;

		@Override
		public void close() throws IOException {
			// nothing to do here
		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		@Override
		public BytesWritable getCurrentValue() throws IOException,
				InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return read ? 1 : 0;
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			read = false;

			fSplit = (FileSplit) split;

			if (fSplit.getLength() > Integer.MAX_VALUE) {
				throw new IOException("Size of file is larger than max integer");
			}

			fs = FileSystem.get(context.getConfiguration());
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (read) {

				// set the key to the fully qualified path
				key.set(fs.makeQualified(fSplit.getPath()).toString());

				int length = (int) fSplit.getLength();

				byte[] bytes = new byte[length];

				// get the bytes of the file for the value
				FSDataInputStream inStream = fs.open(fSplit.getPath());

				IOUtils.readFully(inStream, bytes, 0, length);
				inStream.close();

				// set the value to the byte array
				value.set(bytes, 0, bytes.length);

				read = true;
				return true;
			} else {
				return false;
			}
		}
	}
}
