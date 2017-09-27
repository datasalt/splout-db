#!/bin/bash

# Download Hadoop and configure it

# Stable Apache 

# Cloudera CDH5 tarball
export HADOOP_REPO="http://archive.cloudera.com/cdh5/cdh/5/"
export HADOOP_FILE="hadoop-2.3.0-cdh5.0.0"
export HADOOP_EXT=".tar.gz"


./integrate_test_hadoop_base_mr2.sh