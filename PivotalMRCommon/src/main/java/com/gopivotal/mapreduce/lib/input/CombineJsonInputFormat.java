package com.gopivotal.mapreduce.lib.input;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.gopivotal.mapreduce.lib.input.JsonInputFormat.JsonRecordReader;
import com.gopivotal.mapreduce.lib.input.JsonInputFormat.SimpleJsonRecordReader;
import com.gopivotal.mapreduce.util.PathUtil;

/**
 * An extension of JsonInputFormat that will combine files together to make
 * larger input splits.<br>
 * <br>
 * Please use {@link JsonInputFormat} to configure the job appropriately.
 * {@link PathUtil#getIdealSplitSize(FileSystem, Path[], int)} may be helpful.
 */
public class CombineJsonInputFormat extends
		CombineFileInputFormat<Text, NullWritable> {

	@Override
	public RecordReader<Text, NullWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException {

		if (JsonInputFormat.getOneRecordPerLine(context.getConfiguration())) {
			return new CombineSimpleJsonRecordReader();
		} else {
			return new CombineJsonRecordReader();
		}
	}

	public static class CombineJsonRecordReader extends
			RecordReader<Text, NullWritable> {

		private JsonRecordReader rdr = null;
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
		public NullWritable getCurrentValue() throws IOException,
				InterruptedException {
			return rdr.getCurrentValue();
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return (currentSplit - 1 + rdr.getProgress()) / split.getNumPaths();
		}

		private void initializeNextReader() throws IOException,
				InterruptedException {

			rdr = new JsonRecordReader();
			rdr.initialize(
					new FileSplit(split.getPath(currentSplit), split
							.getOffset(currentSplit), split
							.getLength(currentSplit), null), context);

			++currentSplit;
		}
	}

	public static class CombineSimpleJsonRecordReader extends
			RecordReader<Text, NullWritable> {

		private SimpleJsonRecordReader rdr = null;
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
		public NullWritable getCurrentValue() throws IOException,
				InterruptedException {
			return rdr.getCurrentValue();
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return (currentSplit - 1 + rdr.getProgress()) / split.getNumPaths();
		}

		private void initializeNextReader() throws IOException,
				InterruptedException {

			rdr = new SimpleJsonRecordReader();
			rdr.initialize(
					new FileSplit(split.getPath(currentSplit), split
							.getOffset(currentSplit), split
							.getLength(currentSplit), null), context);

			++currentSplit;
		}
	}
}