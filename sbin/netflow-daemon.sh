#!/usr/bin/env bash
#
# Copyright 2015 ICT.
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Runs a NetFlow command as a daemon.
#
# Environment Variables
#
#   NETFLOW_CONF_DIR  Alternate conf dir. Default is ${NETFLOW_HOME}/conf.
#   NETFLOW_LOG_DIR   Where log files are stored. ${NETFLOW_HOME}/logs by default.
#   NETFLOW_QUERY_MASTER    host:path where NETFLOW code should be rsync'd from
#   NETFLOW_PID_DIR   The pid files are stored. /tmp by default.
#   NETFLOW_IDENT_STRING   A string representing this instance of NETFLOW. $USER by default
#   NETFLOW_NICENESS The scheduling priority for daemons. Defaults to 0.
##

usage="Usage: netflow-daemon.sh [--config <conf-dir>] (start|stop) <netflow-command> <netflow-instance-number> <args...>"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

. "$sbin/netflow-config.sh"

# get arguments

# Check if --config is passed as an argument. It is an optional parameter.
# Exit if the argument is not a directory.

if [ "$1" == "--config" ]
then
  shift
  conf_dir="$1"
  if [ ! -d "$conf_dir" ]
  then
    echo "ERROR : $conf_dir is not a directory"
    echo $usage
    exit 1
  else
    export NETFLOW_CONF_DIR="$conf_dir"
  fi
  shift
fi

option=$1
shift
command=$1
shift
instance=$1
shift

netflow_rotate_log ()
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

. "$NETFLOW_PREFIX/bin/load-netflow-env.sh"

if [ "$NETFLOW_IDENT_STRING" = "" ]; then
  export NETFLOW_IDENT_STRING="$USER"
fi

export NETFLOW_PRINT_LAUNCH_COMMAND="1"

# get log directory
if [ "$NETFLOW_LOG_DIR" = "" ]; then
  export NETFLOW_LOG_DIR="$NETFLOW_HOME/logs"
fi

mkdir -p "NETFLOW_LOG_DIR"
touch "NETFLOW_LOG_DIR"/.netflow_test > /dev/null 2>&1
TEST_LOG_DIR=$?
if [ "${TEST_LOG_DIR}" = "0" ]; then
  rm -f "NETFLOW_LOG_DIR"/.netflow_test
else
  chown "$NETFLOW_IDENT_STRING" "NETFLOW_LOG_DIR"
fi

if [ "$NETFLOW_PID_DIR" = "" ]; then
  $NETFLOW_PID_DIR=/tmp
fi

# some variables
log="NETFLOW_LOG_DIR/netflow-$NETFLOW_IDENT_STRING-$command-$instance-$HOSTNAME.out"
pid="$NETFLOW_PID_DIR/netflow-$NETFLOW_IDENT_STRING-$command-$instance.pid"

# Set default scheduling priority
if [ "$NETFLOW_NICENESS" = "" ]; then
    export NETFLOW_NICENESS=0
fi

case $option in

  (start)

    mkdir -p "$NETFLOW_PID_DIR"

    if [ -f $pid ]; then
      TARGET_ID="$(cat "$pid")"
      if [[ $(ps -p "$TARGET_ID" -o args=) =~ $command ]]; then
        echo "$command running as process $TARGET_ID.  Stop it first."
        exit 1
      fi
    fi

    if [ "$NETFLOW_QUERY_MASTER" != "" ]; then
      echo rsync from "$NETFLOW_QUERY_MASTER"
      rsync -a -e ssh --delete --exclude=.svn --exclude='logs/*' "$NETFLOW_QUERY_MASTER/" "$NETFLOW_HOME"
    fi

    netflow_rotate_log "$log"
    echo "starting $command, logging to $log"
    nohup nice -n $NETFLOW_NICENESS "$NETFLOW_PREFIX"/bin/netflow-class $command "$@" >> "$log" 2>&1 < /dev/null &
    newpid=$!
    echo $newpid > $pid
    sleep 2
    # Check if the process has died; in that case we'll tail the log so the user can see
    if [[ ! $(ps -p "$newpid" -o args=) =~ $command ]]; then
      echo "failed to launch $command:"
      tail -2 "$log" | sed 's/^/  /'
      echo "full log in $log"
    fi
    ;;

  (stop)

    if [ -f $pid ]; then
      TARGET_ID="$(cat "$pid")"
      if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]]; then
        echo "stopping $command"
        kill "$TARGET_ID" && rm -f "$pid"
      else
        echo "no $command to stop"
      fi
    else
      echo "no $command to stop"
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac