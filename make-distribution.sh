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

set -o pipefail
set -e
set -x

NETFLOW_HOME="$(cd "`dirname "$0"`"; pwd)"
DISTDIR="$NETFLOW_HOME/dist"
MVN=`which mvn`

function exit_with_usage {
  echo ""
  echo "make-distribution.sh - tool for making binary distributions of NetFlow"
  cl_options="[--tgz]"
  echo ""
  echo "usage: ./make-distribution.sh $cl_options"
  echo ""
  exit 1
}

# Parse arguments
while (( "$#" )); do
  case $1 in
  --tgz)
    MAKE_TGZ=true
    ;;
  --spark)
    SPARK_DIST="$2"
    shift
    ;;
  --help)
    exit_with_usage
    ;;
  *)
    break
    ;;
  esac
  shift
done

VERSION=$("$MVN" help:evaluate -Dexpression=project.version 2>/dev/null | grep -v "INFO" | tail -n 1)
SCALA_VERSION=$("$MVN" help:evaluate -Dexpression=scala.binary.version $@ 2>/dev/null\
    | grep -v "INFO"\
    | tail -n 1)

rm -rf "$DISTDIR"
mkdir -p "$DISTDIR/lib"
echo "NETFLOW $VERSION" >> "$DISTDIR/RELEASE"

cp "$NETFLOW_HOME"/assembly/target/*assembly*.jar "$DISTDIR/lib/"
cp "$NETFLOW_HOME"/sparkjob/target/scala-$SCALA_VERSION/sparkjob*.jar "$DISTDIR/lib/"

# Copy other things
mkdir "$DISTDIR"/conf
cp "$NETFLOW_HOME"/conf/*.template "$DISTDIR"/conf
cp "$NETFLOW_HOME/README.md" "$DISTDIR"
cp -r "$NETFLOW_HOME/bin" "$DISTDIR"
cp -r "$NETFLOW_HOME/sbin" "$DISTDIR"
cp -r "$NETFLOW_HOME/docs" "$DISTDIR"
cp -r "$NETFLOW_HOME/dev/json" "$DISTDIR"

if [ -n "$SPARK_DIST" ]; then
  cp -r "$SPARK_DIST" "$DISTDIR"
fi

if [ "$MAKE_TGZ" == "true" ]; then
  TARDIR_NAME=netflow-$VERSION-bin
  TARDIR="$NETFLOW_HOME/$TARDIR_NAME"
  rm -rf "$TARDIR"
  cp -r "$DISTDIR" "$TARDIR"
  tar czf "netflow-$VERSION-bin.tgz" -C "$NETFLOW_HOME" "$TARDIR_NAME"
  rm -rf "$TARDIR"
fi
