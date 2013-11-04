package com.gopivotal.mapreduce.lib.driver;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import com.gopivotal.mapreduce.lib.input.CombineTextInputFormat;

/**
 * An extension of TextInputFormat that will combine files together to make
 * larger input splits.
 */
public abstract class AbstractMapReduceDriver extends Configured implements
		Tool {

	private static final String HELP_OPT = "help";
	private static final char NUM_MAPPERS_OPT = 'm';
	private static final char NUM_REDUCERS_OPT = 'n';
	private static final char INPUT_OPT = 'i';
	private static final char OUTPUT_OPT = 'o';

	protected FileSystem fs = null;
	protected Path[] input = null;
	protected Path outputDir = null;
	protected int numReducers = 0, numMappers = 0;

	@Override
	public int run(String[] args) throws Exception {

		CommandLine cmd = parseCommandLine(args);

		if (cmd == null) {
			return 1;
		}

		fs = FileSystem.get(getConf());

		String[] paths = cmd.getOptionValue(INPUT_OPT).split(",");

		input = new Path[paths.length];
		int i = 0;
		for (String p : paths) {
			input[i++] = new Path(p);
		}

		if (cmd.hasOption(OUTPUT_OPT)) {
			outputDir = new Path(cmd.getOptionValue(OUTPUT_OPT));
		}
		
		numReducers = Integer.parseInt(cmd.getOptionValue(NUM_REDUCERS_OPT,
				"-1"));
		numMappers = Integer.parseInt(cmd.getOptionValue(NUM_MAPPERS_OPT, "0"));

		Job job = buildBaseJob();

		preJobLaunch(cmd, job);

		int code = job.waitForCompletion(true) ? 0 : 1;

		postJobCompletion(job);

		return code;

	}

	private Job buildBaseJob() throws IOException {

		Job job = Job.getInstance(getConf());
		job.setJarByClass(AbstractMapReduceDriver.class);

		FileInputFormat.setInputPaths(job, input);

		if (outputDir != null) {
			FileOutputFormat.setOutputPath(job, outputDir);
		}

		job.setMapperClass(getMapperClass());

		if (getCombinerClass() != null) {
			job.setCombinerClass(getCombinerClass());
		}

		if (getReducerClass() == null) {
			job.setNumReduceTasks(0);
		} else {
			job.setReducerClass(getReducerClass());
			if (numReducers >= 0) {
				job.setNumReduceTasks(numReducers);
			}
		}

		job.setInputFormatClass(getInputFormatClass());

		job.setOutputKeyClass(getOutputKeyClass());
		job.setOutputKeyClass(getOutputValueClass());

		if (numMappers > 0) {
			job.setInputFormatClass(CombineTextInputFormat.class);
			CombineTextInputFormat.setMaxInputSplitSize(job,
					getIdealSplitSize(input, numMappers));
		}

		job.setOutputFormatClass(getOutputFormatClass());

		return job;
	}

	protected void preJobLaunch(CommandLine cmd, Job job) throws Exception {
		// empty
	}

	protected void postJobCompletion(Job job) {
		// empty
	}

	protected Options getAdditionalOptions() {
		return new Options();
	}

	@SuppressWarnings("rawtypes")
	protected Class<? extends Mapper> getMapperClass() {
		return Mapper.class;
	}

	@SuppressWarnings("rawtypes")
	protected Class<? extends Reducer> getReducerClass() {
		return Reducer.class;
	}

	@SuppressWarnings("rawtypes")
	protected Class<? extends Reducer> getCombinerClass() {
		return null;
	}

	@SuppressWarnings("rawtypes")
	protected Class<? extends InputFormat> getInputFormatClass() {
		return TextInputFormat.class;
	}

	@SuppressWarnings("rawtypes")
	protected Class<? extends OutputFormat> getOutputFormatClass() {
		return TextOutputFormat.class;
	}

	@SuppressWarnings("rawtypes")
	protected Class<? extends WritableComparable> getOutputKeyClass() {
		return LongWritable.class;
	}

	protected Class<? extends Writable> getOutputValueClass() {
		return Text.class;
	}

	@SuppressWarnings("static-access")
	protected CommandLine parseCommandLine(String[] args) {

		Options opts = getAdditionalOptions();

		opts.addOption(OptionBuilder.withDescription("Print this help message")
				.withLongOpt(HELP_OPT).create());

		opts.addOption(OptionBuilder
				.withDescription(
						"Specifying this parameter will combine blocks into a set number of map tasks ")
				.hasArg().withLongOpt("nummappers").create(NUM_MAPPERS_OPT));
		opts.addOption(OptionBuilder
				.withDescription(
						"Number of reducers.  Default is based on cluster configuration")
				.hasArg().withLongOpt("numreducers").create(NUM_REDUCERS_OPT));

		opts.addOption(OptionBuilder.withDescription("CSV list of input")
				.hasArg().isRequired().withLongOpt("input").create(INPUT_OPT));

		opts.addOption(OptionBuilder.withDescription("Output directory")
				.hasArg().withLongOpt("output").create(OUTPUT_OPT));

		CommandLineParser parser = new GnuParser();

		try {
			CommandLine cmd = parser.parse(opts, args);

			if (cmd.hasOption(HELP_OPT)) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("hadoop jar hawq-errorcheck.jar", opts);
				return null;
			} else {
				return cmd;
			}
		} catch (ParseException e) {
			System.out.println(e.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("hadoop jar hawq-errorcheck.jar [opts]", opts);
			return null;
		}
	}

	private long getIdealSplitSize(Path[] input, int numMapTasks)
			throws IOException {

		long size = 0;

		for (Path status : input) {
			size += getIdealSplitSizeHelper(fs.getFileStatus(status), 0);
		}

		return (long) ((float) size / (float) numMapTasks);
	}

	private long getIdealSplitSizeHelper(FileStatus path, long accum)
			throws IOException {

		if (path.isFile()) {
			accum += path.getLen();
		} else {
			for (FileStatus status : fs.listStatus(path.getPath())) {
				accum += getIdealSplitSizeHelper(status, accum);
			}
		}
		return accum;
	}
}
