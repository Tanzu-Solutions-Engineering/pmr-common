package com.gopivotal.examples.mapreduce;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import com.gopivotal.mapreduce.lib.driver.AbstractMapReduceDriver;
import com.gopivotal.mapreduce.lib.input.DummyInputFormat;

@SuppressWarnings("rawtypes")
public class SleepDriver extends AbstractMapReduceDriver {

	private static final String NUM_MAPPERS_OPT = "m";
	private static final String TIME_IN_MS_OPT = "s";

	@SuppressWarnings("static-access")
	@Override
	protected Options getAdditionalOptions() {
		Options opts = new Options();
		opts.addOption(OptionBuilder.isRequired().hasArg()
				.withLongOpt("nummappers")
				.withDescription("The number of mappers to create to sleep")
				.create(NUM_MAPPERS_OPT));
		opts.addOption(OptionBuilder
				.hasArg()
				.withLongOpt("time-in-ms")
				.withDescription(
						"The amount of time to sleep for.  Default is infinite (-1)")
				.create(TIME_IN_MS_OPT));
		return opts;
	}

	@Override
	protected void preJobLaunch(CommandLine cmd, Job job) throws Exception {
		DummyInputFormat.setNumMappers(job,
				Integer.parseInt(cmd.getOptionValue(NUM_MAPPERS_OPT)));
		if (cmd.hasOption(TIME_IN_MS_OPT)) {
			SleepMapper.setSleepTime(job,
					Long.parseLong(cmd.getOptionValue(TIME_IN_MS_OPT)));
		}
	}

	@Override
	protected Class<? extends InputFormat> getInputFormatClass() {
		return DummyInputFormat.class;
	}

	@Override
	protected Class<? extends OutputFormat> getOutputFormatClass() {
		return NullOutputFormat.class;
	}

	@Override
	protected Class<? extends Mapper> getMapperClass() {
		return SleepMapper.class;
	}

	@Override
	protected boolean isMapOnly() {
		return true;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new SleepDriver(), args);
	}

	public static class SleepMapper extends
			Mapper<Object, Object, Object, Object> {

		public static final String SLEEP_TIME = "mapreduce.sleepmapper.sleep.time";
		private long sleepTime = 0L;

		public static void setSleepTime(Job job, long timeInMs) {
			job.getConfiguration().set(SLEEP_TIME, Long.toString(timeInMs));
		}

		public static long getSleepTime(Configuration conf) throws IOException {
			return conf.getLong(SLEEP_TIME, -1L);
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			sleepTime = getSleepTime(context.getConfiguration());
		}

		@Override
		protected void map(Object key, Object value, Context context)
				throws IOException, InterruptedException {

			if (sleepTime > 0) {
				Thread.sleep(sleepTime);
			} else {
				while (true) {
					Thread.sleep(Long.MAX_VALUE);
				}
			}
		}
	}
}
