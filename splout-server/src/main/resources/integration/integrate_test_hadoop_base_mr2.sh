#!/bin/bash

#
# This script requires the environment variables YARN_HOME, HADOOP_FILE, HADOOP_EXT to be configured before calling it.
# 
# To integrate-test Splout with the stable Apache distro, please configure the variables as:
#
export HADOOP_REPO="http://mirror.arcor-online.net/www.apache.org/hadoop/common/hadoop-2.2.0/"
export HADOOP_FILE="hadoop-2.2.0"
export HADOOP_EXT=".tar.gz"

HELP='This script requires the environment variables YARN_HOME, HADOOP_FILE, HADOOP_EXT to be configured before calling it.'

if [ -z "$HADOOP_REPO" ]; then
    echo $HELP
    exit -1
fi
if [ -z "$HADOOP_FILE" ]; then
    echo $HELP
    exit -1
fi
if [ -z "$HADOOP_EXT" ]; then
    echo $HELP
    exit -1
fi

# Download Hadoop and configure it

SPLOUT_HOME=`pwd`/../

echo "Downloading and configuring Hadoop..."

if [ ! -f $HADOOP_FILE$HADOOP_EXT ]
then
	wget "$HADOOP_REPO$HADOOP_FILE$HADOOP_EXT"
fi

if [ ! -d $HADOOP_FILE ]
then 
	tar xvfz "$HADOOP_FILE$HADOOP_EXT"
	# This is different in MR2 : use /etc/hadoop
	rm $HADOOP_FILE/etc/hadoop/core-site.xml
	rm $HADOOP_FILE/etc/hadoop/hdfs-site.xml
	cp integrate_test_hadoop_confs_mr2/* $HADOOP_FILE/etc/hadoop
fi

export YARN_HOME=`pwd`/$HADOOP_FILE
export HADOOP_CONF_DIR="${YARN_HOME}/etc/hadoop"
export HADOOP_COMMON_HOME="${YARN_HOME}"
export HADOOP_HDFS_HOME="${YARN_HOME}"
export HADOOP_MAPRED_HOME="${YARN_HOME}"

# Format data folder

if [ -d "/tmp/dfs-2" ]
then
	echo "Deleting /tmp/dfs-2/namenode"
	rm -rf /tmp/dfs-2/namenode
	echo "Deleting /tmp/dfs-2/datanode"
	rm -rf /tmp/dfs-2/datanode
fi

# This is different in MR2
$YARN_HOME/bin/hdfs namenode -format

# Start services

echo "Starting YARN / Hadoop..."
cd $YARN_HOME
sbin/hadoop-daemon.sh start namenode
sbin/hadoop-daemon.sh start datanode
sbin/yarn-daemon.sh start resourcemanager
sbin/yarn-daemon.sh start nodemanager

# Start Splout

echo "Starting Splout..."
cd $SPLOUT_HOME
bin/splout-service.sh qnode start
bin/splout-service.sh dnode start

echo "Sleeping 60 seconds to make sure Hadoop starts properly..."
sleep 60

# Run integration test

echo "Running integration test..."
cd $SPLOUT_HOME
$YARN_HOME/bin/hadoop jar splout-hadoop-*-hadoop*.jar integrationtest -i examples/pagecounts/pagecounts-sample/pagecounts-20090430-230000-sample

# Stop Splout

echo "Stopping Splout..."
cd $SPLOUT_HOME
bin/splout-service.sh qnode stop
bin/splout-service.sh dnode stop

# Stop Hadoop

echo "Stopping Hadoop..."
cd $YARN_HOME
sbin/hadoop-daemon.sh stop namenode
sbin/hadoop-daemon.sh stop datanode
sbin/yarn-daemon.sh stop resourcemanager
sbin/yarn-daemon.sh stop nodemanager
