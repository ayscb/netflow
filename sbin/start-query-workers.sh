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

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

. "$sbin/netflow-config.sh"

. "$NETFLOW_PREFIX/bin/load-netflow-env.sh"

# Find the port number for the master
if [ "$NETFLOW_QUERY_MASTER_PORT" = "" ]; then
  NETFLOW_QUERY_MASTER_PORT=9099
fi

if [ "$NETFLOW_QUERY_MASTER_HOST" = "" ]; then
  NETFLOW_QUERY_MASTER_HOST="`hostname`"
fi

# Launch the query workers
exec "$sbin/query-workers.sh" cd "$NETFLOW_HOME" \; "$sbin/start-query-worker.sh" 1 "netflow-query://$NETFLOW_QUERY_MASTER_HOST:$NETFLOW_QUERY_MASTER_PORT"
