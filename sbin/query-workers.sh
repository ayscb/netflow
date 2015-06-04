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


# Run a shell command on all query worker hosts.
#
# Environment Variables
#
#   NETFLOW_QUERY_WORKERS    File naming remote hosts.
#     Default is ${NETFLOW_CONF_DIR}/query-workers.
#   NETFLOW_CONF_DIR  Alternate conf dir. Default is ${NETFLOW_HOME}/conf.
#   NETFLOW_QUERY_WORKER_SLEEP Seconds to sleep between spawning remote commands.
#   NETFLOW_SSH_OPTS Options passed to ssh when running remote commands.
##

usage="Usage: query-workers.sh [--config <conf-dir>] command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

. "$sbin/netflow-config.sh"

# If the query-workers file is specified in the command line,
# then it takes precedence over the definition in
# netflow-env.sh. Save it here.
if [ -f "$NETFLOW_QUERY_WORKERS" ]; then
  HOSTLIST=`cat "$NETFLOW_QUERY_WORKERS"`
fi

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

. "$NETFLOW_PREFIX/bin/load-netflow-env.sh"

if [ "$HOSTLIST" = "" ]; then
  if [ "$NETFLOW_QUERY_WORKERS" = "" ]; then
    if [ -f "${NETFLOW_CONF_DIR}/query-workers" ]; then
      HOSTLIST=`cat "${NETFLOW_CONF_DIR}/query-workers"`
    else
      HOSTLIST=localhost
    fi
  else
    HOSTLIST=`cat "${NETFLOW_QUERY_WORKERS}"`
  fi
fi



# By default disable strict host key checking
if [ "$NETFLOW_SSH_OPTS" = "" ]; then
  NETFLOW_SSH_OPTS="-o StrictHostKeyChecking=no"
fi

for qworker in `echo "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`; do
  if [ -n "${NETFLOW_SSH_FOREGROUND}" ]; then
    ssh $NETFLOW_SSH_OPTS "$qworker" $"${@// /\\ }" \
      2>&1 | sed "s/^/$qworker: /"
  else
    ssh $NETFLOW_SSH_OPTS "$qworker" $"${@// /\\ }" \
      2>&1 | sed "s/^/$qworker: /" &
  fi
  if [ "$NETFLOW_SLAVE_SLEEP" != "" ]; then
    sleep $NETFLOW_SLAVE_SLEEP
  fi
done

wait