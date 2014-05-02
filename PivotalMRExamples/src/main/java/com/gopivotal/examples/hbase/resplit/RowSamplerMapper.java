package com.gopivotal.examples.hbase.resplit;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import com.gopivotal.examples.hbase.util.TwitterHBaseModel;

/**
 * This {@link TableMapper} implementation will emit 1% of all create and delete
 * flags.
 */
public class RowSamplerMapper extends TableMapper<Text, Text> {

	public static final String SAMPLE_RATE = "mapreduce.rowsamplermapper.sample.rate";
	public static Text CREATE = new Text("c");
	public static Text DELETE = new Text("d");
	private Text outvalue = new Text();

	private double sampleRate = .01;
	private Random rndm = new Random();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		sampleRate = getSampleRate(context.getConfiguration());
	}

	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context)
			throws IOException, InterruptedException {

		// if this is a create tweet and we are below our emit threshold
		if (value.getColumnLatestCell(TwitterHBaseModel.COLUMN_FAMILY,
				TwitterHBaseModel.IS_DELETED_CQ) == null
				&& rndm.nextDouble() < sampleRate) {

			// then write this row id out with the 'create' flag
			outvalue.set(Bytes.toString(value.getRow()));
			context.write(CREATE, outvalue);

		} else if (rndm.nextDouble() < sampleRate) {
			// else, this is a delete tweet and we are below our emit threshold
			outvalue.set(Bytes.toString(value.getRow()));
			context.write(DELETE, outvalue);
		}
	}

	/**
	 * Sets the sampling rate percentage in the job configuration, 0 to 1.
	 * Default is .01 (1%)
	 * 
	 * @param job
	 *            The job object
	 * @param rate
	 *            The sampling rate
	 */
	public static void setSampleRate(Job job, double rate) {
		job.getConfiguration().set(SAMPLE_RATE, Double.toString(rate));
	}

	/**
	 * Gets the sampling rate percentage from the given configuration, or .01 if
	 * not set.
	 * 
	 * @param conf
	 *            The job configuration
	 * @return The sampling rate
	 */
	public static double getSampleRate(Configuration conf) {
		return Double.parseDouble(conf.get(SAMPLE_RATE, ".01"));
	}

}