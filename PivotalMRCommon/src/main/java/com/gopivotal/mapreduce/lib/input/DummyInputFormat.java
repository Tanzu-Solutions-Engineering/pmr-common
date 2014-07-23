package com.gopivotal.mapreduce.lib.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * This input format starts a configurable number of map tasks. Both the key and
 * value are {@link NullWritable} objects. It outputs a single key/value pair
 * before closing.
 */
public class DummyInputFormat extends InputFormat<NullWritable, NullWritable> {

	public static final String NUM_MAPPERS = "mapreduce.dummyinputformat.num.mappers";

	public static void setNumMappers(Job job, int numMappers) {
		job.getConfiguration().set(NUM_MAPPERS, Integer.toString(numMappers));
	}

	public static int getNumMappers(Configuration conf) throws IOException {
		String strNumMappers = conf.get(NUM_MAPPERS);
		if (strNumMappers != null) {
			return Integer.parseInt(strNumMappers);
		} else {
			throw new IOException(NUM_MAPPERS + " is not set.");
		}
	}

	@Override
	public RecordReader<NullWritable, NullWritable> createRecordReader(
			InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new DummyRecordReader();
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {

		int numMappers = getNumMappers(context.getConfiguration());

		List<InputSplit> splits = new ArrayList<InputSplit>(numMappers);
		for (int i = 0; i < numMappers; ++i) {
			splits.add(new DummyInputSplit());
		}
		return splits;
	}

	public static class DummyInputSplit extends InputSplit implements Writable {

		@Override
		public void readFields(DataInput arg0) throws IOException {
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
		}

		@Override
		public long getLength() throws IOException, InterruptedException {
			return 0;
		}

		@Override
		public String[] getLocations() throws IOException, InterruptedException {
			return new String[0];
		}

	}

	public static class DummyRecordReader extends
			RecordReader<NullWritable, NullWritable> {

		private boolean output = false;
		private boolean finished = false;

		@Override
		public void initialize(InputSplit arg0, TaskAttemptContext arg1)
				throws IOException, InterruptedException {
			output = false;
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (!output) {
				output = true;
				return true;
			} else {
				return false;
			}
		}

		@Override
		public void close() throws IOException {
			finished = true;
		}

		@Override
		public NullWritable getCurrentKey() throws IOException,
				InterruptedException {
			return NullWritable.get();
		}

		@Override
		public NullWritable getCurrentValue() throws IOException,
				InterruptedException {
			return NullWritable.get();
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return finished ? 1.0f : 0.0f;
		}
	}
}
