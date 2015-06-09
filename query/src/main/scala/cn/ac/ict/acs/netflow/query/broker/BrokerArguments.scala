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
package cn.ac.ict.acs.netflow.query.broker

import cn.ac.ict.acs.netflow.NetFlowConf
import cn.ac.ict.acs.netflow.util.{ IntParam, Utils }

class BrokerArguments(args: Array[String], conf: NetFlowConf) {

  var host = Utils.localHostName()
  var port = 0
  var restPort = 19999
  var masters: Array[String] = _
  var propertiesFile: String = _

  // Check for settings in environment variables
  if (System.getenv("NETFLOW_BROKER_PORT") != null) {
    port = System.getenv("NETFLOW_BROKER_PORT").toInt
  }
  if (System.getenv("NETFLOW_BROKER_REST_PORT") != null) {
    restPort = System.getenv("NETFLOW_BROKER_REST_PORT").toInt
  }

  parse(args.toList)

  propertiesFile = Utils.loadDefaultProperties(conf, propertiesFile)

  def parse(args: List[String]): Unit = args match {
    case ("--host" | "-h") :: value :: tail =>
      Utils.checkHost(value, "Please use hostname " + value)
      host = value
      parse(tail)

    case ("--port" | "-p") :: IntParam(value) :: tail =>
      port = value
      parse(tail)

    case "--rest-port" :: IntParam(value) :: tail =>
      restPort = value
      parse(tail)

    case ("--properties-file") :: value :: tail =>
      propertiesFile = value
      parse(tail)

    case ("--help") :: tail =>
      printUsageAndExit(0)

    case value :: tail =>
      if (masters != null) { // Two positional arguments were given
        printUsageAndExit(1)
      }
      masters = value.stripPrefix("netflow-query://").split(",").map("netflow-query://" + _)
      parse(tail)

    case Nil =>
      if (masters == null) { // No positional argument was given
        printUsageAndExit(1)
      }

    case _ =>
      printUsageAndExit(1)
  }

  /**
   * Print usage and exit JVM with the given exit code.
   */
  def printUsageAndExit(exitCode: Int) {
    System.err.println(
      "Usage: QueryWorker [options] <master>\n" +
        "\n" +
        "Master must be a URL of the form netflow-query://hostname:port\n" +
        "\n" +
        "Options:\n" +
        "  -h HOST, --host HOST     Hostname to listen on\n" +
        "  -p PORT, --port PORT     Port to listen on (default: random)\n" +
        "  --rest-port PORT         Port for web UI (default: 19999)\n" +
        "  --properties-file FILE   Path to a custom netflow properties file.\n" +
        "                           Default is conf/netflow-defaults.conf.")
    System.exit(exitCode)
  }
}
