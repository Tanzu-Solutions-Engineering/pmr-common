package com.gopivotal.mapreduce.lib.output;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.Logger;

/**
 * A HAWQ output format that uses a JDBC connection.
 */
public class HawqOutputFormat extends OutputFormat<Text, Object> {

	public static final String DATABASE = "mapreduce.hawq.output.format.database";
	public static final String TABLE = "mapreduce.hawq.output.format.table";
	public static final String USER = "mapreduce.hawq.output.format.user";
	public static final String PASSWORD = "mapreduce.hawq.output.format.password";
	public static final String HOST = "mapreduce.hawq.output.format.host";
	public static final String PORT = "mapreduce.hawq.output.format.port";
	public static final String TYPES = "mapreduce.hawq.output.format.types";
	public static final String DELIMITER = "mapreduce.hawq.output.format.delimiter";

	public static void setDatabase(Job job, String database) {
		job.getConfiguration().set(DATABASE, database);
	}

	public static String getDatabase(Configuration conf) {
		return conf.get(DATABASE);
	}

	public static void setTable(Job job, String table) {
		job.getConfiguration().set(TABLE, table);
	}

	public static String getTable(Configuration conf) {
		return conf.get(TABLE);
	}

	public static void setUser(Job job, String user) {
		job.getConfiguration().set(USER, user);
	}

	public static String getUser(Configuration conf) {
		return conf.get(USER);
	}

	public static void setPassword(Job job, String password) {
		job.getConfiguration().set(PASSWORD, password);
	}

	public static String getPassword(Configuration conf) {
		return conf.get(PASSWORD);
	}

	public static void setHost(Job job, String host) {
		job.getConfiguration().set(HOST, host);
	}

	public static String getHost(Configuration conf) {
		return conf.get(HOST);
	}

	public static void setPort(Job job, int port) {
		job.getConfiguration().setInt(PORT, port);
	}

	public static int getPort(Configuration conf) {
		return conf.getInt(PORT, 5432);
	}

	public static void setColumnTypes(Job job, Integer... types) {

		if (types == null || types.length == 0) {
			throw new RuntimeException("Type array is null or empty");
		}

		StringBuilder bldr = new StringBuilder();
		for (Integer i : types) {
			bldr.append(i);
			bldr.append(",");
		}

		bldr.deleteCharAt(bldr.length() - 1);
		job.getConfiguration().set(TYPES, bldr.toString());
	}

	public static Integer[] getColumnTypes(Configuration conf) {

		String types = conf.get(TYPES);
		Integer[] retval = new Integer[types.split(",").length];

		int i = 0;
		for (String s : types.split(",")) {
			retval[i++] = Integer.parseInt(s);
		}

		return retval;
	}

	public static void setDelimiter(Job job, String optionValue) {
		job.getConfiguration().set(DELIMITER, optionValue);
	}

	public static String getDelimiter(Configuration job) {
		return job.get(DELIMITER, "\\|");
	}

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException,
			InterruptedException {

		if (HawqOutputFormat.getDatabase(context.getConfiguration()) == null
				|| HawqOutputFormat.getDatabase(context.getConfiguration())
						.isEmpty()) {
			throw new IOException(DATABASE + " is not set");
		}

		if (HawqOutputFormat.getTable(context.getConfiguration()) == null
				|| HawqOutputFormat.getTable(context.getConfiguration())
						.isEmpty()) {
			throw new IOException(TABLE + " is not set");
		}

		if (HawqOutputFormat.getUser(context.getConfiguration()) == null
				|| HawqOutputFormat.getUser(context.getConfiguration())
						.isEmpty()) {
			throw new IOException(USER + " is not set");
		}

		if (HawqOutputFormat.getPassword(context.getConfiguration()) == null
				|| HawqOutputFormat.getPassword(context.getConfiguration())
						.isEmpty()) {
			throw new IOException(PASSWORD + " is not set");
		}

		if (HawqOutputFormat.getHost(context.getConfiguration()) == null
				|| HawqOutputFormat.getHost(context.getConfiguration())
						.isEmpty()) {
			throw new IOException(HOST + " is not set");
		}
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new NullOutputFormat<Text, Object>().getOutputCommitter(context);
	}

	@Override
	public RecordWriter<Text, Object> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new HawqRecordWriter(context.getConfiguration());
	}

	public static class HawqRecordWriter extends RecordWriter<Text, Object> {

		private static Logger LOG = Logger.getLogger(HawqRecordWriter.class);

		private String table, user, password, delimiter, url, sqlStatement;
		private Integer[] types = null;

		private Connection client = null;
		private PreparedStatement insert = null;

		public HawqRecordWriter(Configuration conf) {

			try {
				// Load JDBC driver for Postgres
				Class.forName("org.postgresql.Driver");
				LOG.info("Loaded postgres JDBC driver");
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(
						"Postgres JDBC driver not on classpath");
			}

			// Get all configuration variables
			String hostname = HawqOutputFormat.getHost(conf);
			int port = HawqOutputFormat.getPort(conf);
			String database = HawqOutputFormat.getDatabase(conf);

			table = HawqOutputFormat.getTable(conf);
			user = HawqOutputFormat.getUser(conf);
			password = HawqOutputFormat.getPassword(conf);
			delimiter = HawqOutputFormat.getDelimiter(conf);

			// Parse the types from the configuration
			types = HawqOutputFormat.getColumnTypes(conf);

			// Log the properties
			LOG.info("Properties: " + conf);

			// Create the connect string and SQL statement
			url = "jdbc:postgresql://" + hostname + ":" + port + "/" + database;

			sqlStatement = "INSERT INTO " + table + " VALUES (";

			for (int i = 0; i < types.length; ++i) {
				sqlStatement += "?,";
			}

			sqlStatement = sqlStatement.substring(0, sqlStatement.length() - 1);
			sqlStatement += ");";

			LOG.info("Statement: " + sqlStatement);
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			LOG.info("Pushing the batch and closing client.");

			try {
				insert.executeBatch();
				client.commit();
				insert.close();
				client.close();
			} catch (SQLException e) {
				LOG.error(e);
				throw new IOException(e);
			}
		}

		@Override
		public void write(Text key, Object value) throws IOException,
				InterruptedException {

			try {
				// Verify we have a connection
				verifyConnection();

				String line = key.toString();
				String[] tokens;

				// If we have only one column, set the token to the line
				if (types.length == 1) {
					tokens = new String[] { line };
				} else {
					// Else, we split on our delimiter
					tokens = line.split(delimiter);
				}

				// if we have the right number of tokens (columns)
				if (tokens.length == types.length) {

					// Set our insert statement based on the types
					for (int i = 1; i <= tokens.length; ++i) {
						switch (types[i - 1]) {
						case Types.VARCHAR:
						case Types.CHAR:
							insert.setString(i, tokens[i - 1]);
							break;
						case Types.NUMERIC:
							insert.setBigDecimal(i, new BigDecimal(
									tokens[i - 1]));
							break;
						case Types.INTEGER:
							insert.setInt(i, Integer.parseInt(tokens[i - 1]));
							break;
						case Types.SMALLINT:
							insert.setShort(i, Short.parseShort(tokens[i - 1]));
							break;
						case Types.BIGINT:
							insert.setLong(i, Long.parseLong(tokens[i - 1]));
							break;
						case Types.REAL:
						case Types.FLOAT:
							insert.setFloat(i, Float.parseFloat(tokens[i - 1]));
							break;
						case Types.DOUBLE:
							insert.setDouble(i,
									Double.parseDouble(tokens[i - 1]));
							break;
						case Types.DATE:
							insert.setDate(i, Date.valueOf(tokens[i - 1]));
							break;
						case Types.TIME:
							insert.setTime(i, Time.valueOf(tokens[i - 1]));
							break;
						case Types.TIMESTAMP:
							insert.setTimestamp(i,
									Timestamp.valueOf(tokens[i - 1]));
							break;
						default:
							throw new IOException("Type for " + tokens[i - 1]
									+ " is not yet implemented");
						}
					}

					// Add this statement to the batch
					insert.addBatch();
				}
			} catch (SQLException e) {
				LOG.error(e);
				throw new IOException(e);
			}
		}

		/**
		 * Verifies that a connection is valid and will open one if needed.
		 * 
		 * @throws SQLException
		 */
		private void verifyConnection() throws SQLException {
			if (client == null) {
				openConnection();
			} else if (client.isClosed()) {
				destroyConnection();
				openConnection();
			}
		}

		/**
		 * Opens the given connection and creates the prepared statement
		 * 
		 * @throws SQLException
		 */
		private void openConnection() throws SQLException {
			Properties props = new Properties();

			if (user != null && password != null) {
				props.setProperty("user", user);
				props.setProperty("password", password);
			} else if (user != null ^ password != null) {
				LOG.warn("User or password is set without the other. Continuing with no login auth");
			}

			client = DriverManager.getConnection(url, props);
			client.setAutoCommit(false);
			insert = client.prepareStatement(sqlStatement);
			LOG.info("Opened client connection and prepared insert statement");
		}

		/**
		 * Destroys the current JDBC connection, if any.
		 * 
		 * @throws SQLException
		 */
		private void destroyConnection() throws SQLException {
			if (client != null) {
				client.close();
				client = null;
				LOG.info("Closed client connection");
			}
		}
	}
}
