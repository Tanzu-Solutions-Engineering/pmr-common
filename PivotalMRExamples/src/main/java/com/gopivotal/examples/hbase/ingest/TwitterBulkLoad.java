package com.gopivotal.examples.hbase.ingest;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.util.ToolRunner;

import com.gopivotal.mapreduce.lib.driver.AbstractMapReduceDriver;
import com.gopivotal.mapreduce.lib.input.CombineJsonInputFormat;

/**
 * This class will bulk load an HBase table of tweets and then bulk load an
 * input JSON data set into the table.<br>
 * <br>
 * It is assumed the table has already been created.
 */
public class TwitterBulkLoad extends AbstractMapReduceDriver {

	private HTable htable = null;

	private static final char HTABLE_OPT = 't';

	@Override
	protected void preJobLaunch(CommandLine cmd, Job job) throws Exception {
		job.setJobName("Twitter HBase Bulk Load");
		htable = new HTable(getConf(), cmd.getOptionValue(HTABLE_OPT));

		HFileOutputFormat.configureIncrementalLoad(job, htable);
		HFileOutputFormat.setOutputPath(job, outputDir);
	}

	@Override
	protected void postJobCompletion(Job job) {

		// If job is successful, load it into HBase
		try {
			if (job.isSuccessful()) {
				LoadIncrementalHFiles loader = new LoadIncrementalHFiles(
						getConf());
				loader.doBulkLoad(outputDir, htable);
				System.out.println("MapReduce and bulk load successful");
			} else {
				System.err
						.println("MapReduce job failed.  Skipping bulk load.");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("static-access")
	@Override
	protected Options getAdditionalOptions() {
		Options opts = new Options();
		opts.addOption(OptionBuilder.isRequired().hasArg()
				.withLongOpt("htable")
				.withDescription("The HTable to bulk load into")
				.create(HTABLE_OPT));
		return opts;
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected Class<? extends CombineFileInputFormat> getCombineFileInputFormatClass() {
		return CombineJsonInputFormat.class;
	}

	@Override
	protected Class<?> getJarByClass() {
		return getClass();
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected Class<? extends Mapper> getMapperClass() {
		return TwitterKeyValueMapper.class;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(
				HBaseConfiguration.create(new Configuration()),
				new TwitterBulkLoad(), args));
	}
}