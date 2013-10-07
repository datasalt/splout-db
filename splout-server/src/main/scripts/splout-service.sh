#!/usr/bin/env bash

#
# The Splout server daemon
#
# Environment variables:
#
# SPLOUT_HOME		Where Splout Server installation is. PWD by default.
# SPLOUT_PID_DIR		Where PID file is stored. SPLOUT_HOME by default.
# SPLOUT_LOG_DIR		Where Standard out / err output file is stored. SPLOUT_HOME/logs by default.
# SPLOUT_CLASSPATH	Can be used to include libraries in the Splout classpath
# 
# For Hadoop 1.0:
# ---------------
# HADOOP_HOME
# SPLOUT_HADOOP_CONF_DIR	Optionally, specify the Hadoop configuration folder. Will default to $HADOOP_HOME/conf
#
# For Hadoop 2.0 (YARN / CDH4MR1):
# ----------------------
# SPLOUT_HADOOP_COMMON_HOME	Where the hadoop-common-*.jar can be found.
# SPLOUT_HADOOP_HDFS_HOME	Where the hadoop-mapreduce-client-*.jar can be found.
# SPLOUT_HADOOP_MAPRED_HOME	Where the hadoop-common-*.jar can be found.
# SPLOUT_HADOOP_CONF_DIR	Optionally, specify the Hadoop configuration folder (e.g. /etc/hadoop/conf)
#	

CLASSPATH=""

#
# Env. variables - defaults
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

if [ "$SPLOUT_PID_DIR" = "" ]; then
  export SPLOUT_PID_DIR=$SPLOUT_HOME
fi

if [ "$SPLOUT_LOG_DIR" = "" ]; then
  export SPLOUT_LOG_DIR="$SPLOUT_HOME/logs"
fi

JAVA_LIBRARY_PATH=${SPLOUT_HOME}/native

rotate_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
	num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
	while [ $num -gt 1 ]; do
	    prev=`expr $num - 1`
	    [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
	    num=$prev
	done
	mv "$log" "$log.$num";
    fi
}

usage="splout-server [qnode|dnode] [start|stop]"
service=$1
command="Splout $service"
pid="$SPLOUT_PID_DIR/splout-$service.pid"
log="$SPLOUT_LOG_DIR/splout-$service.out"
className=""
startStop=$2
case $service in
	(dnode)
		className="com.splout.db.dnode.DNode"    	
	;; 
	(qnode)
		className="com.splout.db.qnode.QNode"    			
	;;
	(*)
esac

if [ "$startStop" == "start" ]; then
	if [ -z "$HADOOP_HOME" ]; then
	    if [ -z "$SPLOUT_HADOOP_MAPRED_HOME" ]; then
	        echo "Required env variables not found: HADOOP_HOME for Hadoop 1.0 or SPLOUT_HADOOP_MAPRED_HOME, SPLOUT_HADOOP_HDFS_HOME & SPLOUT_HADOOP_COMMON_HOME for Hadoop 2.0."
	        exit 1
	    else
	    	echo "Using defined Hadoop 2.0 environment variable SPLOUT_HADOOP_MAPRED_HOME"
	    fi
	else 
		if [ -z "$SPLOUT_HADOOP_MAPRED_HOME" ] && [ -z "$SPLOUT_HADOOP_HDFS_HOME" ] && [ -z "$SPLOUT_HADOOP_COMMON_HOME" ] ; then
			echo "Using defined Hadoop 1.0 environment variable HADOOP_HOME: $HADOOP_HOME." 
		else
			echo "Both HADOOP_HOME and (SPLOUT_HADOOP_MAPRED_HOME or SPLOUT_HADOOP_HDFS_HOME or SPLOUT_HADOOP_COMMON_HOME) are defined which may lead to inconsistencies in the classpath. Please use one or the other, but not both."
			exit 1
		fi
	fi
	
	# Hadoop 1.0
	if [ "$HADOOP_HOME" ]; then
		echo "Loading appropriate jars from HADOOP_HOME: $HADOOP_HOME"
		HADOOP_JARS="$HADOOP_HOME/hadoop-*.jar $HADOOP_HOME/lib/guava-*.jar"
		for f in $HADOOP_JARS
		do
		        HADOOP_JAR_CS="$HADOOP_JAR_CS:$f"
		done
		CLASSPATH=${CLASSPATH}:$HADOOP_JAR_CS:$HADOOP_HOME/conf
	fi
	# Hadoop 2.0
	if [ "$SPLOUT_HADOOP_MAPRED_HOME" ]; then
		echo "Loading appropriate jars from SPLOUT_HADOOP_MAPRED_HOME: $SPLOUT_HADOOP_MAPRED_HOME"
		HADOOP_JARS="$SPLOUT_HADOOP_MAPRED_HOME/hadoop-*.jar $SPLOUT_HADOOP_MAPRED_HOME/lib/guava-*.jar $SPLOUT_HADOOP_HDFS_HOME/lib/commons-cli*.jar $SPLOUT_HADOOP_HDFS_HOME/lib/protobuf*.jar"
		for f in $HADOOP_JARS
		do
		        HADOOP_JAR_CS="$HADOOP_JAR_CS:$f"
		done
		CLASSPATH=${CLASSPATH}:$HADOOP_JAR_CS:$SPLOUT_HADOOP_MAPRED_HOME/conf
		if [ "$SPLOUT_HADOOP_COMMON_HOME" ]; then
			echo "Loading appropriate jars from SPLOUT_HADOOP_COMMON_HOME: $SPLOUT_HADOOP_COMMON_HOME"
			HADOOP_JARS="$SPLOUT_HADOOP_COMMON_HOME/hadoop-*.jar $SPLOUT_HADOOP_COMMON_HOME/lib/hadoop-*.jar"
			for f in $HADOOP_JARS
			do
			        HADOOP_JAR_CS="$HADOOP_JAR_CS:$f"
			done
			CLASSPATH=${CLASSPATH}:$HADOOP_JAR_CS
		else
			echo "SPLOUT_HADOOP_COMMON_HOME is not defined, Splout may not behave well if not being able to load all libraries from a YARN installation. Please fix your environment."
			exit 1
		fi
		if [ "$SPLOUT_HADOOP_HDFS_HOME" ]; then
			echo "Loading appropriate jars from SPLOUT_HADOOP_HDFS_HOME: $SPLOUT_HADOOP_HDFS_HOME"
			HADOOP_JARS="$SPLOUT_HADOOP_HDFS_HOME/hadoop-*.jar"
			for f in $HADOOP_JARS
			do
			        HADOOP_JAR_CS="$HADOOP_JAR_CS:$f"
			done
			CLASSPATH=${CLASSPATH}:$HADOOP_JAR_CS
		else
			echo "SPLOUT_HADOOP_HDFS_HOME is not defined, Splout may not behave well if not being able to load all libraries from a YARN installation. Please fix your environment."
			exit 1
		fi
	fi

	if [ "$SPLOUT_HADOOP_CONF_DIR" ]; then
		echo "Adding specified Hadoop config folder to classpath: $SPLOUT_HADOOP_CONF_DIR"
		CLASSPATH=${CLASSPATH}:${SPLOUT_HADOOP_CONF_DIR}
	fi

	# SPLOUT_CLASSPATH
	if [ "$SPLOUT_CLASSPATH" ]; then
		echo "Loading libraries from env vaiable SPLOUT_CLASSPATH: $SPLOUT_CLASSPATH"
		CLASSPATH=${CLASSPATH}:$SPLOUT_CLASSPATH
	fi

	CLASSPATH=${CLASSPATH}:$SPLOUT_HOME/*:$SPLOUT_HOME/lib/*
fi

case $startStop in

  (start)

    mkdir -p "$SPLOUT_LOG_DIR"

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo $command running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi
    
    rotate_log $log
    echo "Starting $command, logging to $log - PID file in $pid"
    cd $SPLOUT_HOME
    nohup java -classpath "$CLASSPATH" -Djava.library.path=$JAVA_LIBRARY_PATH $className > "$log" 2>&1 < /dev/null &
    echo $! > $pid
    sleep 1; head "$log"
    ;;

  (stop)

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo Stopping $command
        kill `cat $pid`
      else
        echo No $command to stop
      fi
    else
      echo No $command to stop
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac
