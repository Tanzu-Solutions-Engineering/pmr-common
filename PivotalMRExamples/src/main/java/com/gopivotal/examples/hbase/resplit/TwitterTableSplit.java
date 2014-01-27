package com.gopivotal.examples.hbase.resplit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.gopivotal.examples.hbase.util.TwitterHBaseModel;

/**
 * This Driver class splits an HBase table that is using an
 * {@link TwitterHBaseModel}. It has two distinct phases: <br>
 * <ol>
 * <li>Creating two HBase tables: one for the 'create' status updates and one
 * for the 'delete' status updates. First, the ideal region splits are
 * determined for both tables. The splits are determined by randomly sampling
 * the input table and getting equi-distant keys.The number of regions is passed
 * as a command line parameter.</li>
 * <li>Running two parallel MapReduce analytics to bulk load both tables into
 * HBase. This will filter out any rows that do not belong in the destination
 * table.</li>
 * </ol>
 * The end result is two ideally split HBase tables.
 */
public class TwitterTableSplit extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 6) {
			System.err
					.println("Parameters: <input-table> <create-table-name> <delete-table-name> <numregions> <sample-rate>");
			return 1;
		}

		// Get all command line arguments
		String inputTable = args[0];
		String createTableName = args[1];
		String deleteTableName = args[2];
		Path outputDir = new Path(args[3]);
		int numRegions = Integer.parseInt(args[4]);
		double sampleRate = Double.parseDouble(args[5]);

		// Create two working directories for our bulk load operation
		Path createTableWorkingDir = new Path(outputDir, "create-work");
		Path deleteTableWorkingDir = new Path(outputDir, "delete-work");

		// Create two new pre-split tables by sampling the original table
		int code = createNewTablesFromSample(new Configuration(getConf()),
				inputTable, outputDir, numRegions, sampleRate, createTableName,
				deleteTableName);

		// if this operation was successful, then bulk load both tables
		if (code == 0) {
			// Start two jobs which will run in parallel
			Job createJob = startBulkLoad(new Configuration(getConf()),
					inputTable, createTableName, CreateRowMapper.class,
					createTableWorkingDir);

			Job deleteJob = startBulkLoad(new Configuration(getConf()),
					inputTable, deleteTableName, DeleteRowMapper.class,
					deleteTableWorkingDir);

			// Sleep until both jobs are complete
			while (!createJob.isComplete() && !deleteJob.isComplete()) {
				Thread.sleep(1000);
			}

			// If both jobs are successful, complete the bulk load
			if (createJob.isSuccessful() && deleteJob.isSuccessful()) {
				LoadIncrementalHFiles loader = new LoadIncrementalHFiles(
						getConf());

				loader.doBulkLoad(createTableWorkingDir, new HTable(getConf(),
						createTableName));
				loader.doBulkLoad(deleteTableWorkingDir, new HTable(getConf(),
						deleteTableName));

				System.out.println("Bulk load of two tables complete");
			} else {
				System.err
						.println("One of the bulk load jobs has failed.  Exiting");
			}
		}

		return code;
	}

	private int createNewTablesFromSample(Configuration conf,
			String inputTable, Path outputDir, int numRegions,
			double sampleRate, String createTableName, String deleteTableName)
			throws Exception {

		// Create our job to sample the HBase row keyspace
		Job job = Job.getInstance(conf, "HBase Row Sampler");
		job.setJarByClass(getClass());

		// Initialize our mapper by specifying the input table
		TableMapReduceUtil.initTableMapperJob(inputTable, new Scan(),
				RowSamplerMapper.class, Text.class, Text.class, job);

		// Set our sampling rate
		RowSamplerMapper.setSampleRate(job, sampleRate);

		// Set our reducer and one task
		job.setReducerClass(RowSamplerReducer.class);
		job.setNumReduceTasks(1);

		// Configure both of our outputs for the "create" and "delete" tweets
		MultipleOutputs.addNamedOutput(job, "create", TextOutputFormat.class,
				Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "delete", TextOutputFormat.class,
				Text.class, NullWritable.class);

		// Configure our output directory
		TextOutputFormat.setOutputPath(job, outputDir);

		// launch the job
		int code = job.waitForCompletion(true) ? 0 : 1;

		if (code == 0) {
			System.out.println("Sample job succeeded.  Creating new tables");

			byte[][] regionSplits = getNRegionsFromFile(conf, numRegions,
					new Path(outputDir, "create-r-00000"));

			makeHBaseTable(conf, createTableName, regionSplits);

			regionSplits = getNRegionsFromFile(conf, numRegions, new Path(
					outputDir, "delete-r-00000"));

			makeHBaseTable(conf, deleteTableName, regionSplits);

			System.out.println("Tables created.  Beginning bulk ingestion.");

			return code;
		} else {
			System.err.println("Sample job failed.");
			return code;
		}
	}

	private byte[][] getNRegionsFromFile(Configuration conf, int numRegions,
			Path path) throws IOException {

		System.out.println("Getting " + (numRegions - 1) + " splits from "
				+ path.toString());
		FileSystem fs = FileSystem.get(conf);

		BufferedReader rdr = new BufferedReader(new InputStreamReader(
				fs.open(path)));

		String line;
		int numLines = 0;
		while ((line = rdr.readLine()) != null) {
			++numLines;
		}

		byte[][] retval = new byte[numRegions - 1][];

		rdr = new BufferedReader(new InputStreamReader(fs.open(path)));

		int stepSize = numLines / (numRegions - 1);
		int step = stepSize;
		System.out.println("Total lines: " + numLines + "  Step size: " + step);

		numLines = 0;
		int arrayIndex = 0;

		while ((line = rdr.readLine()) != null) {
			++numLines;

			if (step == numLines) {
				retval[arrayIndex++] = Bytes.toBytes(line.trim());
				if (arrayIndex >= retval.length) {
					break;
				}

				step += stepSize;
			}
		}

		return retval;
	}

	private void makeHBaseTable(Configuration conf, String tableName,
			byte[][] regionSplits) throws IOException {

		System.out.println("Creating table " + tableName);

		HTableDescriptor newTable = new HTableDescriptor(tableName);
		newTable.addFamily(new HColumnDescriptor(
				TwitterHBaseModel.COLUMN_FAMILY));

		HBaseAdmin admin = new HBaseAdmin(conf);

		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}

		admin.createTable(newTable, regionSplits);
		admin.close();
	}

	@SuppressWarnings("rawtypes")
	private Job startBulkLoad(Configuration conf, String inputTable,
			String tableName, Class<? extends TableMapper> clazz, Path outputDir)
			throws Exception {

		// Create our job to bulk load into HBase
		Job job = Job.getInstance(conf, "HBase Bulk Loader");
		job.setJarByClass(getClass());

		// Initialize our mapper by specifying the input table
		TableMapReduceUtil.initTableMapperJob(inputTable, new Scan(), clazz,
				ImmutableBytesWritable.class, KeyValue.class, job);

		HFileOutputFormat.configureIncrementalLoad(job, new HTable(conf,
				tableName));
		HFileOutputFormat.setOutputPath(job, outputDir);

		// launch the job
		job.waitForCompletion(true);
		return job;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(
				HBaseConfiguration.create(new Configuration()),
				new TwitterTableSplit(), args));
	}
}
