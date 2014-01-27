package com.gopivotal.examples.hbase.util;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Constants for the HBase columns
 */
public class TwitterHBaseModel {

	public static final byte[] COLUMN_FAMILY = Bytes.toBytes("c");
	public static final byte[] DATE_CQ = Bytes.toBytes("d");
	public static final byte[] IS_DELETED_CQ = Bytes.toBytes("del");
	public static final byte[] SOURCE_CQ = Bytes.toBytes("s");
	public static final byte[] TEXT_CQ = Bytes.toBytes("t");
	public static final byte[] USER_ID_CQ = Bytes.toBytes("u ");

	public static final byte[] TRUE = Bytes.toBytes("true");
	public static final byte[] FALSE = Bytes.toBytes("false");
}
