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

exec "$sbin/load-workers.sh" cd "$NETFLOW_HOME" \; "$sbin/netflow-daemon.sh" "$@"
