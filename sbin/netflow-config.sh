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

# included in all the netflow scripts with source command
# should not be executable directly
# also should not be passed any arguments, since we need original $*

# resolve links - $0 may be a softlink
this="${BASH_SOURCE:-$0}"
common_bin="$(cd -P -- "$(dirname -- "$this")" && pwd -P)"
script="$(basename -- "$this")"
this="$common_bin/$script"

# convert relative path to absolute path
config_bin="`dirname "$this"`"
script="`basename "$this"`"
config_bin="`cd "$config_bin"; pwd`"
this="$config_bin/$script"

export NETFLOW_PREFIX="`dirname "$this"`"/..
export NETFLOW_HOME="${NETFLOW_PREFIX}"
export NETFLOW_CONF_DIR="${NETFLOW_CONF_DIR:-"$NETFLOW_HOME/conf"}"