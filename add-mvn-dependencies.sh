#!/bin/sh

# The default behavior of this script is to search the default PHD installation locations for JAR files.  Declare a local lib directory to look there instead.  The lib directory should only contain jar files, not any sub-directories
#LIB_DIR=lib

HADOOP_VERSION=2.2.0-gphd-3.0.1.0
HAWQ_VERSION=1.2.0.1
HBASE_VERSION=0.96.0-hadoop2-gphd-3.0.1.0
PIG_VERSION=0.12.0-gphd-3.0.1.0
HIVE_VERSION=0.12.0-gphd-3.0.1.0

if [[ -z $LIB_DIR ]]; then
    GPHD_ROOT=/usr/lib/gphd
    HADOOP_ROOT=$GPHD_ROOT/hadoop
    HDFS_ROOT=$GPHD_ROOT/hadoop-hdfs
    MAPREDUCE_ROOT=$GPHD_ROOT/hadoop-mapreduce
    HBASE_ROOT=$GPHD_ROOT/hbase/lib
    PIG_ROOT=$GPHD_ROOT/pig
    HIVE_ROOT=$GPHD_ROOT/hive/lib
    HAWQ_ROOT=/usr/local/hawq/lib/postgresql/hawq-mr-io
else
    HADOOP_ROOT=$LIB_DIR
    HDFS_ROOT=$LIB_DIR
    MAPREDUCE_ROOT=$LIB_DIR
    HBASE_ROOT=$LIB_DIR
    PIG_ROOT=$LIB_DIR
    HAWQ_ROOT=$LIB_DIR
fi

PACKAGING=jar

mvn install:install-file -Dfile=$HADOOP_ROOT/hadoop-common-$HADOOP_VERSION.jar -DgroupId=org.apache.hadoop -DartifactId=hadoop-common -Dversion=$HADOOP_VERSION -Dpackaging=jar &> /dev/null
if [[ $? -ne 0 ]]; then echo "Failed to install $HADOOP_ROOT/hadoop-common-$HADOOP_VERSION.jar... Does it exist?  Exiting."; exit 1; fi

mvn install:install-file -Dfile=$HDFS_ROOT/hadoop-hdfs-$HADOOP_VERSION.jar -DgroupId=org.apache.hadoop -DartifactId=hadoop-hdfs -Dversion=$HADOOP_VERSION -Dpackaging=jar  &> /dev/null
if [[ $? -ne 0 ]]; then echo "Failed to install $HDFS_ROOT/hadoop-hdfs-$HADOOP_VERSION.jar... Does it exist?  Exiting."; exit 1; fi

mvn install:install-file -Dfile=$MAPREDUCE_ROOT/hadoop-mapreduce-client-core-$HADOOP_VERSION.jar -DgroupId=org.apache.hadoop -DartifactId=hadoop-mapreduce-client-core -Dversion=$HADOOP_VERSION -Dpackaging=jar &> /dev/null
if [[ $? -ne 0 ]]; then echo "Failed to install $MAPREDUCE_ROOT/hadoop-mapreduce-client-core-$HADOOP_VERSION.jar... Does it exist?  Exiting."; exit 1; fi

mvn install:install-file -Dfile=$MAPREDUCE_ROOT/hadoop-mapreduce-client-common-$HADOOP_VERSION.jar -DgroupId=org.apache.hadoop -DartifactId=hadoop-mapreduce-client-common -Dversion=$HADOOP_VERSION -Dpackaging=jar &> /dev/null
if [[ $? -ne 0 ]]; then echo "Failed to install $MAPREDUCE_ROOT/hadoop-mapreduce-client-common-$HADOOP_VERSION.jar... Does it exist?  Exiting."; exit 1; fi

mvn install:install-file -Dfile=$HBASE_ROOT/hbase-common-$HBASE_VERSION.jar -DgroupId=org.apache.hbase -DartifactId=hbase-common -Dversion=$HBASE_VERSION -Dpackaging=jar &> /dev/null
if [[ $? -ne 0 ]]; then echo "Failed to install $HBASE_ROOT/hbase-common-$HBASE_VERSION.jar... Does it exist?  Exiting."; exit 1; fi

mvn install:install-file -Dfile=$HBASE_ROOT/hbase-client-$HBASE_VERSION.jar -DgroupId=org.apache.hbase -DartifactId=hbase-client -Dversion=$HBASE_VERSION -Dpackaging=jar &> /dev/null
if [[ $? -ne 0 ]]; then echo "Failed to install $HBASE_ROOT/hbase-client-$HBASE_VERSION.jar... Does it exist?  Exiting."; exit 1; fi

mvn install:install-file -Dfile=$HBASE_ROOT/hbase-server-$HBASE_VERSION.jar -DgroupId=org.apache.hbase -DartifactId=hbase-server -Dversion=$HBASE_VERSION -Dpackaging=jar &> /dev/null
if [[ $? -ne 0 ]]; then echo "Failed to install $HBASE_ROOT/hbase-server-$HBASE_VERSION.jar... Does it exist?  Exiting."; exit 1; fi

mvn install:install-file -Dfile=$HBASE_ROOT/hbase-hadoop2-compat-$HBASE_VERSION.jar -DgroupId=org.apache.hbase -DartifactId=hbase-hadoop2-compat -Dversion=$HBASE_VERSION -Dpackaging=jar &> /dev/null
if [[ $? -ne 0 ]]; then echo "Failed to install $HBASE_ROOT/hbase-hadoop2-compat-$HBASE_VERSION.jar... Does it exist?  Exiting."; exit 1; fi

mvn install:install-file -Dfile=$HIVE_ROOT/hive-serde-$HIVE_VERSION.jar -DgroupId=org.apache.hadoop -DartifactId=hadoop-common -Dversion=$HADOOP_VERSION -Dpackaging=jar &> /dev/null
if [[ $? -ne 0 ]]; then echo "Failed to install $HIVE_ROOT/hive-serde-$HIVE_VERSION.jar... Does it exist?  Exiting."; exit 1; fi

mvn install:install-file -Dfile=$PIG_ROOT/pig-$PIG_VERSION.jar -DgroupId=org.apache.pig -DartifactId=pig -Dversion=$PIG_VERSION -Dpackaging=jar &> /dev/null
if [[ $? -ne 0 ]]; then echo "Failed to install $PIG_ROOT/pig-$PIG_VERSION.jar... Does it exist?  Exiting."; exit 1; fi

mvn install:install-file -Dfile=$HAWQ_ROOT/hawq-mapreduce-tool.jar -DgroupId=com.gopivotal -DartifactId=hawq-mapreduce-tool -Dversion=$HAWQ_VERSION -Dpackaging=jar &> /dev/null
if [[ $? -ne 0 ]]; then echo "Failed to install $HAWQ_ROOT/hawq-mapreduce-tool.jar... Does it exist?  Exiting."; exit 1; fi

mvn install:install-file -Dfile=$HAWQ_ROOT/hawq-mapreduce-ao.jar -DgroupId=com.gopivotal -DartifactId=hawq-mapreduce-ao -Dversion=$HAWQ_VERSION -Dpackaging=jar &> /dev/null
if [[ $? -ne 0 ]]; then echo "Failed to install $HAWQ_ROOT/hawq-mapreduce-ao.jar... Does it exist?  Exiting."; exit 1; fi

mvn install:install-file -Dfile=$HAWQ_ROOT/hawq-mapreduce-common.jar -DgroupId=com.gopivotal -DartifactId=hawq-mapreduce-common -Dversion=$HAWQ_VERSION -Dpackaging=jar &> /dev/null
if [[ $? -ne 0 ]]; then echo "Failed to install $HAWQ_ROOT/hawq-mapreduce-common.jar... Does it exist?  Exiting."; exit 1; fi

echo "All dependencies added successfully."

exit 0

