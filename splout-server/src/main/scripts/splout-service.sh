#!/usr/bin/env bash

#
# The Splout server daemon
#
# Environment variables:
#
# SPLOUT_HOME		Where Splout Server installation is. PWD by default.
# HADOOP_HOME		Where Hadoop is installed
# SPLOUT_PID_DIR		Where PID file is stored. SPLOUT_HOME by default.
# SPLOUT_LOG_DIR		Where Standard out / err output file is stored. SPLOUT_HOME/logs by default.
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

if [ "$HADOOP_HOME" = "" ]; then
        echo "Required HADOOP_HOME environmental variable not configured."
        exit 1
else
        # Prepend HADOOP_HOME/conf for being able to fetch from HDFS
        # We also prepend ONLY the Hadoop core JAR and the minimun number of jars 
        # for avoiding RPC mismatch problems
        HADOOP_JARS="$HADOOP_HOME/hadoop-*.jar"
        for f in $HADOOP_JARS
        do
                HADOOP_JAR_CS="$HADOOP_JAR_CS:$f"
        done
        CLASSPATH=${CLASSPATH}$HADOOP_JAR_CS:$HADOOP_HOME/conf:$SPLOUT_HOME/*:$SPLOUT_HOME/lib/*
fi

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
