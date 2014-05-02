package com.gopivotal.examples.hbase.resplit;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

import com.gopivotal.examples.hbase.util.TwitterHBaseModel;

/**
 * A {@link TableMapper} instance that will only emit create statuses
 */
public class CreateRowMapper extends
		TableMapper<ImmutableBytesWritable, KeyValue> {

	private ImmutableBytesWritable outkey = new ImmutableBytesWritable();

	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context)
			throws IOException, InterruptedException {

		// if this is a create tweet, i.e. there is no delete flag
		if (value.getColumnLatestCell(TwitterHBaseModel.COLUMN_FAMILY,
				TwitterHBaseModel.IS_DELETED_CQ) == null) {
			for (KeyValue kv : value.raw()) {
				outkey.set(kv.getRow());
				context.write(outkey, kv);
			}
		}
	}
}