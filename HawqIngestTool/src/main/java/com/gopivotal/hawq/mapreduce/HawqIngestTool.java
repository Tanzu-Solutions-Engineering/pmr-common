package com.gopivotal.hawq.mapreduce;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.util.ToolRunner;

import com.gopivotal.mapreduce.lib.driver.AbstractMapReduceDriver;
import com.gopivotal.mapreduce.lib.output.HawqOutputFormat;
import com.gopivotal.mapreduce.util.HawqUtil;

public class HawqIngestTool extends AbstractMapReduceDriver {

	// private static final Logger LOG = Logger.getLogger(HawqIngestTool.class);

	private static final char HOST_OPT = 'h';
	private static final char PORT_OPT = 'p';
	private static final char DATABASE_OPT = 'd';
	private static final char TABLE_OPT = 't';
	private static final char USER_OPT = 'u';
	private static final char PASSWORD_OPT = 's';
	private static final String DELIMITER_OPT = "delimiter";

	@Override
	protected void preJobLaunch(CommandLine cmd, Job job) throws Exception {

		String host = cmd.getOptionValue(HOST_OPT);
		int port = Integer.parseInt(cmd.getOptionValue(PORT_OPT));
		String database = cmd.getOptionValue(DATABASE_OPT);
		String table = cmd.getOptionValue(TABLE_OPT);
		String user = cmd.getOptionValue(USER_OPT);
		String password = cmd.getOptionValue(PASSWORD_OPT);

		// Create the connect string and SQL statement
		String url = "jdbc:postgresql://" + host + ":" + port + "/" + database;

		HawqOutputFormat.setColumnTypes(job,
				HawqUtil.getColumnDefs(url, table, user, password));
		HawqOutputFormat.setDatabase(job, database);
		HawqOutputFormat.setDelimiter(job,
				cmd.getOptionValue(DELIMITER_OPT, "\\|"));
		HawqOutputFormat.setHost(job, host);
		HawqOutputFormat.setPort(job, port);
		HawqOutputFormat.setTable(job, table);

		HawqOutputFormat.setUser(job, user);
		HawqOutputFormat.setPassword(job, password);
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected Class<? extends Mapper> getMapperClass() {
		return InverseMapper.class;
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected Class<? extends Reducer> getReducerClass() {
		return null;
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected Class<? extends OutputFormat> getOutputFormatClass() {
		return HawqOutputFormat.class;
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected Class<? extends WritableComparable> getOutputKeyClass() {
		return Text.class;
	}

	@Override
	protected Class<? extends Writable> getOutputValueClass() {
		return NullWritable.class;
	}

	@SuppressWarnings("static-access")
	protected Options getAdditionalOptions() {

		Options opts = new Options();

		opts.addOption(OptionBuilder.withDescription("HAWQ hostname").hasArg()
				.isRequired().withType(String.class).withLongOpt("host")
				.create(HOST_OPT));

		opts.addOption(OptionBuilder.withDescription("HAWQ port").hasArg()
				.isRequired().withType(Integer.class).withLongOpt("port")
				.create(PORT_OPT));

		opts.addOption(OptionBuilder.withDescription("HAWQ database").hasArg()
				.isRequired().withType(String.class).withLongOpt("database")
				.create(DATABASE_OPT));

		opts.addOption(OptionBuilder.withDescription("HAWQ table").hasArg()
				.isRequired().withLongOpt("table").create(TABLE_OPT));
		opts.addOption(OptionBuilder.withDescription("HAWQ username").hasArg()
				.isRequired().withLongOpt("user").create(USER_OPT));
		opts.addOption(OptionBuilder.withDescription("HAWQ password").hasArg()
				.isRequired().withLongOpt("password").create(PASSWORD_OPT));

		opts.addOption(OptionBuilder
				.withDescription(
						"Delimiter of the rows of data.  Default is pipe - |")
				.hasArg().withLongOpt(DELIMITER_OPT).create());
		return opts;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new HawqIngestTool(),
				args));
	}
}
