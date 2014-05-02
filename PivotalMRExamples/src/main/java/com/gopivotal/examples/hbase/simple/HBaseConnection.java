package com.gopivotal.examples.hbase.simple;

import java.io.BufferedReader;
import java.io.FileReader;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HBaseConnection extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(HBaseConfiguration.create(),
				new HBaseConnection(), args));
	}

	private static final byte[] PROFILE_CF = Bytes.toBytes("profile");
	private static final byte[] TWEETS_CF = Bytes.toBytes("tweets");

	private static final byte[] NAME_CQ = Bytes.toBytes("name");
	private static final byte[] FOLLOWERS_CQ = Bytes.toBytes("followers");
	private static final byte[] CREATED_CQ = Bytes.toBytes("created");

	private static final byte[] TWEET_ID_CQ = Bytes.toBytes("tweetId");
	private static final byte[] TWEET_DATE_CQ = Bytes.toBytes("created");
	private static final byte[] TWEET_SRC_CQ = Bytes.toBytes("tweetSrc");

	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage: users.tsv tweets.tsv");
		}

		String userData = args[0];
		String tweetData = args[1];

		HTableDescriptor ht = new HTableDescriptor(TableName.valueOf("users"));
		ht.addFamily(new HColumnDescriptor(PROFILE_CF));
		ht.addFamily(new HColumnDescriptor(TWEETS_CF));

		System.out.println("Connecting to HBase...");

		System.out.println("Creating Table 'users'");
		HBaseAdmin hba = new HBaseAdmin(getConf());
		hba.createTable(ht);
		hba.close();

		System.out.println("Done.");

		System.out.println("Loading user profile data into HBase");

		HTable table = new HTable(getConf(), "users");

		BufferedReader rdr = new BufferedReader(new FileReader(userData));

		String line;
		while ((line = rdr.readLine()) != null) {
			String[] tokens = line.split("\t");
			String userId = tokens[0];
			String screenName = tokens[1];
			String createDate = tokens[2];
			String numFollowers = tokens[3];

			Put p = new Put(Bytes.toBytes(userId));

			p.add(PROFILE_CF, NAME_CQ, Bytes.toBytes(screenName));
			p.add(PROFILE_CF, CREATED_CQ, Bytes.toBytes(createDate));
			p.add(PROFILE_CF, FOLLOWERS_CQ, Bytes.toBytes(numFollowers));

			table.put(p);
		}

		rdr.close();

		rdr = new BufferedReader(new FileReader(tweetData));

		while ((line = rdr.readLine()) != null) {
			String[] tokens = line.split("\t");
			String tweetId = tokens[0];
			String userId = tokens[1];
			String tweetDate = tokens[2];
			String tweetSource = tokens[3];

			Put p = new Put(Bytes.toBytes(userId));

			p.add(TWEETS_CF, TWEET_ID_CQ, Bytes.toBytes(tweetId));
			p.add(TWEETS_CF, TWEET_DATE_CQ, Bytes.toBytes(tweetDate));
			p.add(TWEETS_CF, TWEET_SRC_CQ, Bytes.toBytes(tweetSource));

			table.put(p);
		}

		rdr.close();

		System.out.println("Loading user tweets into HBase");

		table.close();

		return 0;
	}
}