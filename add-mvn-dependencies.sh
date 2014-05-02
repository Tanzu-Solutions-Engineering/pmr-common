#!/bin/sh

GPHD_ROOT=/usr/lib/gphd
HADOOP_VERSION=2.2.0-gphd-3.0.0.0
HAWQ_VERSION=1.2.1.0
HBASE_VERSION=0.96.0-hadoop2-gphd-3.0.0.0
PIG_VERSION=0.12.0-gphd-3.0.0.0
PACKAGING=jar

mvn install:install-file -Dfile=$GPHD_ROOT/hadoop/hadoop-common-$HADOOP_VERSION.jar -DgroupId=org.apache.hadoop -DartifactId=hadoop-common -Dversion=$HADOOP_VERSION -Dpackaging=jar
mvn install:install-file -Dfile=$GPHD_ROOT/hadoop-hdfs/hadoop-hdfs-$HADOOP_VERSION.jar -DgroupId=org.apache.hadoop -DartifactId=hadoop-hdfs -Dversion=$HADOOP_VERSION -Dpackaging=jar
mvn install:install-file -Dfile=$GPHD_ROOT/hadoop-mapreduce/hadoop-mapreduce-client-core-$HADOOP_VERSION.jar -DgroupId=org.apache.hadoop -DartifactId=hadoop-mapreduce-client-core -Dversion=$HADOOP_VERSION -Dpackaging=jar
mvn install:install-file -Dfile=$GPHD_ROOT/hadoop-mapreduce/hadoop-mapreduce-client-common-$HADOOP_VERSION.jar -DgroupId=org.apache.hadoop -DartifactId=hadoop-mapreduce-client-common -Dversion=$HADOOP_VERSION -Dpackaging=jar

mvn install:install-file -Dfile=$GPHD_ROOT/hbase/lib/hbase-common-$HBASE_VERSION.jar -DgroupId=org.apache.hbase -DartifactId=hbase-common -Dversion=$HBASE_VERSION -Dpackaging=jar
mvn install:install-file -Dfile=$GPHD_ROOT/hbase/lib/hbase-client-$HBASE_VERSION.jar -DgroupId=org.apache.hbase -DartifactId=hbase-client -Dversion=$HBASE_VERSION -Dpackaging=jar
mvn install:install-file -Dfile=$GPHD_ROOT/hbase/lib/hbase-server-$HBASE_VERSION.jar -DgroupId=org.apache.hbase -DartifactId=hbase-server -Dversion=$HBASE_VERSION -Dpackaging=jar
mvn install:install-file -Dfile=$GPHD_ROOT/hbase/lib/hbase-hadoop2-compat-$HBASE_VERSION.jar -DgroupId=org.apache.hbase -DartifactId=hbase-hadoop2-compat -Dversion=$HBASE_VERSION -Dpackaging=jar

mvn install:install-file -Dfile=$GPHD_ROOT/pig/pig-$PIG_VERSION.jar -DgroupId=org.apache.pig -DartifactId=pig -Dversion=$PIG_VERSION -Dpackaging=jar

mvn install:install-file -Dfile=/usr/local/hawq/lib/postgresql/hawq-mr-io/hawq-mapreduce-tool.jar -DgroupId=com.gopivotal -DartifactId=hawq-mapreduce-tool -Dversion=$HAWQ_VERSION -Dpackaging=jar
mvn install:install-file -Dfile=/usr/local/hawq/lib/postgresql/hawq-mr-io/hawq-mapreduce-ao.jar -DgroupId=com.gopivotal -DartifactId=hawq-mapreduce-ao -Dversion=$HAWQ_VERSION -Dpackaging=jar
mvn install:install-file -Dfile=/usr/local/hawq/lib/postgresql/hawq-mr-io/hawq-mapreduce-common.jar -DgroupId=com.gopivotal -DartifactId=hawq-mapreduce-common -Dversion=$HAWQ_VERSION -Dpackaging=jar

