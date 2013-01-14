#!/bin/bash

#
# This script requires the environment variables HADOOP_HOME, HADOOP_FILE, HADOOP_EXT to be configured before calling it.
# 
# To integrate-test Splout with the stable Apache distro, please configure the variables as:
#
# export HADOOP_REPO="http://mirror.arcor-online.net/www.apache.org/hadoop/common/stable/"
# export HADOOP_FILE="hadoop-1.0.4"
# export HADOOP_EXT=".tar.gz"

HELP='This script requires the environment variables HADOOP_HOME, HADOOP_FILE, HADOOP_EXT to be configured before calling it. To integrate-test Splout with the stable Apache distro, please configure the variables as: export HADOOP_REPO="http://mirror.arcor-online.net/www.apache.org/hadoop/common/stable/"; export HADOOP_FILE="hadoop-1.0.4"; export HADOOP_EXT=".tar.gz"'

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
	rm $HADOOP_FILE/conf/core-site.xml
	rm $HADOOP_FILE/conf/hdfs-site.xml
	cp integrate_test_hadoop_confs/* $HADOOP_FILE/conf/
fi

export HADOOP_HOME=`pwd`/$HADOOP_FILE
export HADOOP_CONF_DIR=$HADOOP_HOME/conf

# Format data folder

if [ -d "/tmp/dfs-hadoop" ]
then
	echo "Deleting /tmp/dfs-hadoop"
	rm -rf /tmp/dfs-hadoop
fi
if [ -d "/tmp/tmp-hadoop" ]
then
	echo "Deleting /tmp/tmp-hadoop"
	rm -rf /tmp/tmp-hadoop
fi

$HADOOP_HOME/bin/hadoop namenode -format

# Start services

echo "Starting Hadoop..."
cd $HADOOP_HOME
bin/hadoop-daemon.sh start jobtracker 
bin/hadoop-daemon.sh start namenode
bin/hadoop-daemon.sh start datanode
bin/hadoop-daemon.sh start tasktracker

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
$HADOOP_HOME/bin/hadoop jar splout-hadoop-*-SNAPSHOT-hadoop.jar integrationtest -i examples/pagecounts/pagecounts-sample/pagecounts-20090430-230000-sample

# Stop Splout

echo "Stopping Splout..."
cd $SPLOUT_HOME
bin/splout-service.sh qnode stop
bin/splout-service.sh dnode stop

# Stop Hadoop

echo "Stopping Hadoop..."
cd $HADOOP_HOME
bin/hadoop-daemon.sh stop jobtracker
bin/hadoop-daemon.sh stop namenode
bin/hadoop-daemon.sh stop datanode
bin/hadoop-daemon.sh stop tasktracker
