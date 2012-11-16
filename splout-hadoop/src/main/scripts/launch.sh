#!/usr/bin/env bash
#
# Launches any class Splout class.
# The script takes care of the classpath configuration
#

# Resolve links ($0 may be a softlink) and convert a relative path
# to an absolute path.
this="${BASH_SOURCE-$0}"
bin=$(cd -P -- "$(dirname -- "$this")" && pwd -P)
script="$(basename -- "$this")"
this="$bin/$script"

if [ "$SPLOUT_HOME" = "" ]; then
  export SPLOUT_HOME=`dirname "$this"`/..
fi

if [ "$HADOOP_HOME" = "" ]; then
        echo "Required HADOOP_HOME environmental variable not configured."
        exit 1
else
        # Prepend HADOOP_HOME/conf for being able to fetch from HDFS
        # We also prepend ONLY the Hadoop core JAR and the minimun number of jars 
        # for avoiding RPC mismatch problems
        HADOOP_JARS="$HADOOP_HOME/hadoop-core*.jar $HADOOP_HOME/lib/guava*"
        for f in $HADOOP_JARS
        do
                HADOOP_JAR_CS="$HADOOP_JAR_CS:$f"
        done
	# (Java 6 compliant classpath with * symbols)
        CLASSPATH=${CLASSPATH}$HADOOP_JAR_CS:$HADOOP_HOME/conf:$SPLOUT_HOME/*:$SPLOUT_HOME/lib/*       
fi

java -cp "$CLASSPATH" $*
