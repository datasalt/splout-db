#!/bin/bash

# Download Hadoop and configure it

# Stable Apache 

# Using a Hortonworks compilation because the Apache one gives problems with 64bits
export HADOOP_REPO="http://public-repo-1.hortonworks.com/HDP/centos5/2.x/updates/2.0.6.1/tars/"
export HADOOP_FILE="hadoop-2.2.0.2.0.6.0-102"
export HADOOP_EXT=".tar.gz"

./integrate_test_hadoop_base_mr2.sh