#!/usr/bin/env bash


# Run a command on all query workers.

usage="Usage: netflow-daemons.sh [--config <conf-dir>] [start|stop] command instance-number args..."

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

sbin=`dirname "$0"`
sbin=`cd "$sbin"; pwd`

. "$sbin/netflow-config.sh"

exec "$sbin/query-workers.sh" cd "$NETFLOW_HOME" \; "$sbin/netflow-daemon.sh" "$@"
