package com.gopivotal.mapred.input;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.security.InvalidParameterException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.gopivotal.io.JsonStreamReader;

/**
 * The JsonInputFormat will read two types of JSON formatted data. The default
 * expectation is each JSON record is newline delimited. This method is
 * generally faster and is backed by the {@link LineRecordReader) you are likely
 * familiar with. The other method is 'pretty print' of JSON records, where
 * records span multiple lines and often have some type of root identifier. This
 * method is likely slower, but respects record boundaries much like the
 * LineRecordReader.<br>
 * <br>
 * Use of the 'pretty print' reader requires a record identifier.
 */
public class JsonInputFormat extends FileInputFormat<Text, NullWritable> {

	private static JsonFactory factory = new JsonFactory();
	private static ObjectMapper mapper = new ObjectMapper(factory);

	public static final String ONE_RECORD_PER_LINE = "json.input.format.one.record.per.line";
	public static final String RECORD_IDENTIFIER = "json.input.format.record.identifier";

	@Override
	public RecordReader<Text, NullWritable> getRecordReader(InputSplit split,
			JobConf conf, Reporter reporter) throws IOException {

		if (getOneRecordPerLine(conf)) {
			return new SimpleJsonRecordReader(conf, (FileSplit) split);
		} else {
			return new JsonRecordReader(conf, (FileSplit) split);
		}
	}

	/**
	 * This class uses the {@link LineRecordReader} to read a line of JSON and
	 * return it as a Text object.
	 */
	public static class SimpleJsonRecordReader implements
			RecordReader<Text, NullWritable> {

		private LineRecordReader rdr = null;
		private LongWritable key = new LongWritable();
		private Text value = new Text();

		public SimpleJsonRecordReader(Configuration conf, FileSplit split)
				throws IOException {
			rdr = new LineRecordReader(conf, split);
		}

		@Override
		public void close() throws IOException {
			rdr.close();
		}

		@Override
		public Text createKey() {
			return value;
		}

		@Override
		public NullWritable createValue() {
			return NullWritable.get();
		}

		@Override
		public long getPos() throws IOException {
			return rdr.getPos();
		}

		@Override
		public boolean next(Text key, NullWritable value) throws IOException {
			if (rdr.next(this.key, this.value)) {
				key.set(this.value);
				return true;
			} else {
				return false;
			}
		}

		@Override
		public float getProgress() throws IOException {
			return rdr.getProgress();
		}
	}

	/**
	 * This class uses the {@link JsonStreamReader} to read JSON records from a
	 * file. It respects split boundaries to complete full JSON records, as
	 * specified by the root identifier. This class will discard any records
	 * that it was unable to decode using
	 * {@link JsonInputFormat#decodeLineToJsonNode(String)}
	 */
	public static class JsonRecordReader implements
			RecordReader<Text, NullWritable> {

		private Logger LOG = Logger.getLogger(JsonRecordReader.class);

		private JsonStreamReader rdr = null;
		private long start = 0, end = 0;
		private float toRead = 0;
		private String identifier = null;
		private Logger log = Logger.getLogger(JsonRecordReader.class);

		public JsonRecordReader(JobConf conf, FileSplit split)
				throws IOException {
			this.identifier = conf.get(RECORD_IDENTIFIER);

			if (this.identifier == null || identifier.isEmpty()) {
				throw new InvalidParameterException(
						JsonInputFormat.RECORD_IDENTIFIER + " is not set.");
			} else {
				LOG.info("Initializing JsonRecordReader with identifier "
						+ identifier);
			}

			// get relevant data
			Path file = split.getPath();

			log.info("File is " + file);

			start = split.getStart();
			end = start + split.getLength();
			toRead = end - start;

			FSDataInputStream strm = FileSystem.get(conf).open(file);
			if (start != 0) {
				strm.seek(start);
			}

			rdr = new JsonStreamReader(identifier,
					new BufferedInputStream(strm));
		}

		@Override
		public boolean next(Text key, NullWritable value) throws IOException {

			boolean retval = false;
			boolean keepGoing = false;
			do {
				keepGoing = false;
				String record = rdr.getJsonRecord();
				if (record != null) {
					if (JsonInputFormat.decodeLineToJsonNode(record) == null) {
						keepGoing = true;
					} else {
						key.set(record);
						retval = true;
					}
				}
			} while (keepGoing);

			return retval;
		}

		@Override
		public Text createKey() {
			return new Text();
		}

		@Override
		public NullWritable createValue() {
			return NullWritable.get();
		}

		@Override
		public long getPos() throws IOException {
			return start + rdr.getBytesRead();
		}

		@Override
		public void close() throws IOException {
			rdr.close();
		}

		@Override
		public float getProgress() throws IOException {
			return (float) rdr.getBytesRead() / toRead;
		}
	}

	/**
	 * Decodes a given string of text to a {@link JsonNode}.
	 * 
	 * @param line
	 *            The line of text
	 * @return The JsonNode or null if a JsonParseException,
	 *         JsonMappingException, or IOException error occurs
	 */
	public static synchronized JsonNode decodeLineToJsonNode(String line) {
		try {
			return mapper.readTree(line);
		} catch (JsonParseException e) {
			e.printStackTrace();
			return null;
		} catch (JsonMappingException e) {
			e.printStackTrace();
			return null;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Sets the input format to use the {@link SimpleJsonRecordReader} if true,
	 * otherwise {@link JsonRecordReader}.<br>
	 * <br>
	 * Default is true.
	 * 
	 * @param job
	 *            The job to configure
	 * @param isOneRecordPerLine
	 *            True if JSON records are new line delimited, false otherwise.
	 */
	public static void setOneRecordPerLine(Job job, boolean isOneRecordPerLine) {
		job.getConfiguration().setBoolean(ONE_RECORD_PER_LINE,
				isOneRecordPerLine);
	}

	/**
	 * Gets if this is configured as one JSON record per line.
	 * 
	 * @param conf
	 *            the Job configuration
	 * @return True if one JSON record per line, false otherwise.
	 */
	public static boolean getOneRecordPerLine(Configuration conf) {
		return conf.getBoolean(ONE_RECORD_PER_LINE, true);
	}

	/**
	 * Specifies a record identifier to be used with the
	 * {@link JsonRecordReader}<br>
	 * <br>
	 * Must be set if {@link JsonInputFormat#setOneRecordPerLine} is false.
	 * 
	 * @param job
	 *            The job to configure
	 * @param isOneRecordPerLine
	 *            True if JSON records are new line delimited, false otherwise.
	 */
	public static void setRecordIdentifier(Job job, String record) {
		job.getConfiguration().set(RECORD_IDENTIFIER, record);
	}

	/**
	 * Gets the record identifier
	 * 
	 * @param conf
	 *            the Job configuration
	 * @return The record identifier or null if not set
	 */
	public static String getRecordIdentifier(Configuration conf) {
		return conf.get(RECORD_IDENTIFIER);
	}
}
