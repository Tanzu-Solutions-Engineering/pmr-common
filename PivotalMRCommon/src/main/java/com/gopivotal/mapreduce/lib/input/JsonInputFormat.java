package com.gopivotal.mapreduce.lib.input;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.security.InvalidParameterException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.gopivotal.io.JsonStreamReader;

public class JsonInputFormat extends FileInputFormat<Text, NullWritable> {

	private static JsonFactory factory = new JsonFactory();
	private static ObjectMapper mapper = new ObjectMapper(factory);

	public static final String ONE_RECORD_PER_LINE = "json.input.format.one.record.per.line";
	public static final String RECORD_IDENTIFIER = "json.input.format.record.identifier";

	@Override
	public RecordReader<Text, NullWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		RecordReader<Text, NullWritable> rdr;
		if (context.getConfiguration().getBoolean(ONE_RECORD_PER_LINE, false)) {
			rdr = new SimpleJsonRecordReader();
		} else {
			return new JsonRecordReader();
		}
		rdr.initialize(split, context);
		return rdr;
	}

	public static class SimpleJsonRecordReader extends
			RecordReader<Text, NullWritable> {

		private LineRecordReader rdr = null;
		private Text outkey = new Text();
		private NullWritable outvalue = NullWritable.get();

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {

			rdr = new LineRecordReader();
			rdr.initialize(split, context);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (rdr.nextKeyValue()) {
				outkey.set(rdr.getCurrentValue());
				return true;
			} else {
				return false;
			}
		}

		@Override
		public void close() throws IOException {
			rdr.close();
		}

		@Override
		public float getProgress() throws IOException {
			return rdr.getProgress();
		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			return outkey;
		}

		@Override
		public NullWritable getCurrentValue() throws IOException,
				InterruptedException {
			return outvalue;
		}
	}

	public static class JsonRecordReader extends
			RecordReader<Text, NullWritable> {

		private Logger LOG = Logger.getLogger(JsonRecordReader.class);

		private JsonStreamReader rdr = null;
		private long start = 0, end = 0;
		private float toRead = 0;
		private String identifier = null;
		private Logger log = Logger.getLogger(JsonRecordReader.class);
		private Text outkey = new Text();
		private NullWritable outvalue = NullWritable.get();

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {

			this.identifier = context.getConfiguration().get(RECORD_IDENTIFIER);
			log.info("Identifier is " + this.identifier);

			if (this.identifier == null || identifier.isEmpty()) {
				throw new InvalidParameterException(
						JsonInputFormat.RECORD_IDENTIFIER + " is not set.");
			} else {
				LOG.info("Initializing JsonRecordReader with identifier "
						+ identifier);
			}

			FileSplit fSplit = (FileSplit) split;

			// get relevant data
			Path file = fSplit.getPath();

			log.info("File is " + file);

			start = fSplit.getStart();
			end = start + split.getLength();
			toRead = end - start;

			FSDataInputStream strm = FileSystem.get(context.getConfiguration())
					.open(file);

			log.info("Retrieved file stream ");

			if (start != 0) {
				strm.seek(start);
			}

			rdr = new JsonStreamReader(identifier,
					new BufferedInputStream(strm));

			log.info("Reader is " + rdr);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			boolean retval = false;
			boolean keepGoing = false;
			do {
				keepGoing = false;
				String record = rdr.getJsonRecord();
				if (record != null) {
					if (JsonInputFormat.decodeLineToJsonNode(record) == null) {
						log.error("Unable to parse JSON string.  Skipping. DEBUG to see");
						log.debug(record);
						keepGoing = true;
					} else {
						outkey.set(record);
						retval = true;
					}
				}
			} while (keepGoing);

			return retval;
		}

		@Override
		public void close() throws IOException {
			rdr.close();
		}

		@Override
		public float getProgress() throws IOException {
			return (float) rdr.getBytesRead() / toRead;
		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			return outkey;
		}

		@Override
		public NullWritable getCurrentValue() throws IOException,
				InterruptedException {
			return outvalue;
		}
	}

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
}
