package com.gopivotal.examples.hbase.resplit;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * This reducer class sorts all the input values associated with a key and
 * writes to the appropriate output via MultipleOutputs.
 */
public class RowSamplerReducer extends Reducer<Text, Text, Text, NullWritable> {

	private MultipleOutputs<Text, NullWritable> mos = null;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		mos = new MultipleOutputs<Text, NullWritable>(context);
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		if (key.equals(RowSamplerMapper.CREATE)) {
			ArrayList<Text> list = new ArrayList<Text>();
			for (Text value : values) {
				list.add(new Text(value));
			}

			Collections.sort(list);

			for (Text value : list) {
				mos.write("create", value, NullWritable.get());
			}
		} else {
			ArrayList<Text> list = new ArrayList<Text>();
			for (Text value : values) {
				list.add(new Text(value));
			}

			Collections.sort(list);

			for (Text value : list) {
				mos.write("delete", value, NullWritable.get());
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}
}