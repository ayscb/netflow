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

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

. "$FWDIR"/bin/load-netflow-env.sh

function appendToClasspath(){
  if [ -n "$1" ]; then
    if [ -n "$CLASSPATH" ]; then
      CLASSPATH="$CLASSPATH:$1"
    else
      CLASSPATH="$1"
    fi
  fi
}

# Build up classpath
if [ -n "$NETFLOW_CONF_DIR" ]; then
  appendToClasspath "$NETFLOW_CONF_DIR"
else
  appendToClasspath "$FWDIR/conf"
fi

ASSEMBLY_DIR="$FWDIR/assembly/target/scala-$NETFLOW_SCALA_VERSION"

if [ -n "$JAVA_HOME" ]; then
  JAR_CMD="$JAVA_HOME/bin/jar"
else
  JAR_CMD="jar"
fi

# A developer option to prepend more recently compiled Spark classes
if [ -n "$SPARK_PREPEND_CLASSES" ]; then
  echo "NOTE: SPARK_PREPEND_CLASSES is set, placing locally compiled Spark"\
    "classes ahead of assembly." >&2
  # Spark classes
  appendToClasspath "$FWDIR/core/target/scala-$SPARK_SCALA_VERSION/classes"
  appendToClasspath "$FWDIR/repl/target/scala-$SPARK_SCALA_VERSION/classes"
  appendToClasspath "$FWDIR/mllib/target/scala-$SPARK_SCALA_VERSION/classes"
  appendToClasspath "$FWDIR/bagel/target/scala-$SPARK_SCALA_VERSION/classes"
  appendToClasspath "$FWDIR/graphx/target/scala-$SPARK_SCALA_VERSION/classes"
  appendToClasspath "$FWDIR/streaming/target/scala-$SPARK_SCALA_VERSION/classes"
  appendToClasspath "$FWDIR/tools/target/scala-$SPARK_SCALA_VERSION/classes"
  appendToClasspath "$FWDIR/sql/catalyst/target/scala-$SPARK_SCALA_VERSION/classes"
  appendToClasspath "$FWDIR/sql/core/target/scala-$SPARK_SCALA_VERSION/classes"
  appendToClasspath "$FWDIR/sql/hive/target/scala-$SPARK_SCALA_VERSION/classes"
  appendToClasspath "$FWDIR/sql/hive-thriftserver/target/scala-$SPARK_SCALA_VERSION/classes"
  appendToClasspath "$FWDIR/yarn/stable/target/scala-$SPARK_SCALA_VERSION/classes"
  # Jars for shaded deps in their original form (copied here during build)
  appendToClasspath "$FWDIR/core/target/jars/*"
fi

# Use spark-assembly jar from either RELEASE or assembly directory
if [ -f "$FWDIR/RELEASE" ]; then
  assembly_folder="$FWDIR"/lib
else
  assembly_folder="$ASSEMBLY_DIR"
fi

num_jars=0

for f in "${assembly_folder}"/spark-assembly*hadoop*.jar; do
  if [[ ! -e "$f" ]]; then
    echo "Failed to find Spark assembly in $assembly_folder" 1>&2
    echo "You need to build Spark before running this program." 1>&2
    exit 1
  fi
  ASSEMBLY_JAR="$f"
  num_jars=$((num_jars+1))
done

if [ "$num_jars" -gt "1" ]; then
  echo "Found multiple Spark assembly jars in $assembly_folder:" 1>&2
  ls "${assembly_folder}"/spark-assembly*hadoop*.jar 1>&2
  echo "Please remove all but one jar." 1>&2
  exit 1
fi

# Only able to make this check if 'jar' command is available
if [ $(command -v "$JAR_CMD") ] ; then
  # Verify that versions of java used to build the jars and run Spark are compatible
  jar_error_check=$("$JAR_CMD" -tf "$ASSEMBLY_JAR" nonexistent/class/path 2>&1)
  if [[ "$jar_error_check" =~ "invalid CEN header" ]]; then
    echo "Loading Spark jar with '$JAR_CMD' failed. " 1>&2
    echo "This is likely because Spark was compiled with Java 7 and run " 1>&2
    echo "with Java 6. (see SPARK-1703). Please use Java 7 to run Spark " 1>&2
    echo "or build Spark with Java 6." 1>&2
    exit 1
  fi
fi

appendToClasspath "$ASSEMBLY_JAR"

# Add test classes if we're running from SBT or Maven with SPARK_TESTING set to 1
if [[ $SPARK_TESTING == 1 ]]; then
  appendToClasspath "$FWDIR/core/target/scala-$SPARK_SCALA_VERSION/test-classes"
  appendToClasspath "$FWDIR/repl/target/scala-$SPARK_SCALA_VERSION/test-classes"
  appendToClasspath "$FWDIR/mllib/target/scala-$SPARK_SCALA_VERSION/test-classes"
  appendToClasspath "$FWDIR/bagel/target/scala-$SPARK_SCALA_VERSION/test-classes"
  appendToClasspath "$FWDIR/graphx/target/scala-$SPARK_SCALA_VERSION/test-classes"
  appendToClasspath "$FWDIR/streaming/target/scala-$SPARK_SCALA_VERSION/test-classes"
  appendToClasspath "$FWDIR/sql/catalyst/target/scala-$SPARK_SCALA_VERSION/test-classes"
  appendToClasspath "$FWDIR/sql/core/target/scala-$SPARK_SCALA_VERSION/test-classes"
  appendToClasspath "$FWDIR/sql/hive/target/scala-$SPARK_SCALA_VERSION/test-classes"
fi

echo "$CLASSPATH"