/**
 * Copyright 2015 ICT.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.ac.ict.acs

package object netflow {

  val NETFLOW_VERSION = "1.0-SNAPSHOT"
  val SPARK_VERSION = "1.4.0-netflow"
  val HADOOP_VERSION = "2.4.0"

  val SCALA_VERSION = "2.10.4"
  val SCALA_BINARY_VERSION = "2.10"

  val QUERYMASTER_ACTORSYSTEM = "netflowQueryMaster"
  val QUERYMASTER_ACTOR = "QueryMaster"
  val BROKER_ACTORSYSTEM = "netflowRest"
  val BROKER_ACTOR = "RestBroker"
}
