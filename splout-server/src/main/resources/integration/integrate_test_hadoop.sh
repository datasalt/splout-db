#!/bin/bash

# Download Hadoop and configure it

# Stable Apache 

export HADOOP_REPO="http://archive.apache.org/dist/hadoop/core/hadoop-1.0.4/"
export HADOOP_FILE="hadoop-1.0.4"
export HADOOP_EXT=".tar.gz"

./integrate_test_hadoop_base.sh

# Cloudera's CDH3

export HADOOP_REPO="http://archive.cloudera.com/cdh/3/"
export HADOOP_FILE="hadoop-0.20.2-cdh3u5"
export HADOOP_EXT=".tar.gz"

./integrate_test_hadoop_base.sh
