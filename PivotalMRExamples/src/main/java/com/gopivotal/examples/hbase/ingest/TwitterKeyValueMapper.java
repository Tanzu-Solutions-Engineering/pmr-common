package com.gopivotal.examples.hbase.ingest;

import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.gopivotal.examples.hbase.util.TwitterHBaseModel;

import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.TwitterException;
import twitter4j.json.DataObjectFactory;

/**
 * Mapper class to extract data from a JSON tweet and output an HBase
 * {@link KeyValue} object. Uses the {@link TwitterHBaseModel} class for the
 * column definitions.<br>
 * <br>
 * Utilizes twitter4j to do the object extraction
 * 
 * @see <a href="http://twitter4j.org">http://twitter4j.org</a>
 * 
 */
public class TwitterKeyValueMapper extends
		Mapper<Text, NullWritable, ImmutableBytesWritable, KeyValue> {

	private SimpleDateFormat twitterDateFormat = new SimpleDateFormat(
			"EEE MMM dd HH:mm:ss ZZZZZ yyyy");
	private ImmutableBytesWritable outkey = new ImmutableBytesWritable();

	@Override
	protected void map(Text key, NullWritable value, Context context)
			throws IOException, InterruptedException {

		try {

			Object obj;
			// Create
			try {
				obj = DataObjectFactory.createObject(key.toString());
			} catch (OutOfMemoryError e) {
				System.err.println(key.getLength());
				System.err.println(key.toString());
				System.gc();
				return;
			}

			if (obj instanceof Status) {
				Status status = (Status) obj;

				// get status id
				byte[] id = Bytes.toBytes(Long.toString(status.getId()));

				// write source
				KeyValue kv = new KeyValue(id, TwitterHBaseModel.COLUMN_FAMILY,
						TwitterHBaseModel.SOURCE_CQ, Bytes.toBytes(status
								.getSource()));
				writeKeyValue(context, kv);

				// write text
				kv = new KeyValue(id, TwitterHBaseModel.COLUMN_FAMILY,
						TwitterHBaseModel.TEXT_CQ, Bytes.toBytes(status
								.getText()));
				writeKeyValue(context, kv);

				// write userId
				kv = new KeyValue(id, TwitterHBaseModel.COLUMN_FAMILY,
						TwitterHBaseModel.USER_ID_CQ, Bytes.toBytes(Long
								.toString(status.getUser().getId())));
				writeKeyValue(context, kv);

				// write date
				kv = new KeyValue(id, TwitterHBaseModel.COLUMN_FAMILY,
						TwitterHBaseModel.DATE_CQ,
						Bytes.toBytes(twitterDateFormat.format(status
								.getCreatedAt())));
				writeKeyValue(context, kv);

			} else if (obj instanceof StatusDeletionNotice) {
				StatusDeletionNotice delete = (StatusDeletionNotice) obj;

				// write that this record was deleted
				writeKeyValue(
						context,
						new KeyValue(Bytes.toBytes(Long.toString(delete
								.getStatusId())),
								TwitterHBaseModel.COLUMN_FAMILY,
								TwitterHBaseModel.IS_DELETED_CQ,
								TwitterHBaseModel.TRUE));
			}
		} catch (TwitterException e) {
			context.getCounter("Exceptions", "Twitter").increment(1);
		}
	}

	/**
	 * Helper function to write a {@link KeyValue} pair out of the mapper
	 * 
	 * @param context
	 *            The Context object from the mapper
	 * @param kv
	 *            The KeyValue to emit
	 */
	private void writeKeyValue(Context context, KeyValue kv)
			throws IOException, InterruptedException {
		outkey.set(kv.getRowArray());
		context.write(outkey, kv);
	}
}
