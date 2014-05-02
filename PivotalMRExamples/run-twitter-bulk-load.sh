#!/bin/sh

TABLE_NAME=tweets
# Create tweets table
#echo "create '$TABLE_NAME', 'c'" | hbase shell

#export HBASE_CLASSPATH=$HBASE_CLASSPATH:`pwd`/target/pmr-examples-*.jar

# Put data in HDFS
#hdfs dfs -put twitter-sample.json .

Compatibility Factory?
# Run bulk load
hbase classpath
hadoop jar target/pmr-examples-*.jar com.gopivotal.examples.hbase.ingest.TwitterBulkLoad -i twitter-sample.json -o hbase-work -htable $TABLE_NAME

exit 0
