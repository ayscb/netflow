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
package cn.ac.ict.acs.netflow.deploy.qmaster

import cn.ac.ict.acs.netflow.NetFlowConf
import cn.ac.ict.acs.netflow.util.{IntParam, Utils}

class QueryMasterArguments(args: Array[String], conf: NetFlowConf) {
  var host = Utils.localHostName()
  var port = 9099
  var webUiPort = 18080
  var propertiesFile: String = null

  // Check for settings in environment variables
  if (System.getenv("NETFLOW_QUERY_MASTER_HOST") != null) {
    host = System.getenv("NETFLOW_QUERY_MASTER_HOST")
  }
  if (System.getenv("NETFLOW_QUERY_MASTER_PORT") != null) {
    port = System.getenv("NETFLOW_QUERY_MASTER_PORT").toInt
  }
  if (System.getenv("NETFLOW_QUERY_MASTER_WEBUI_PORT") != null) {
    webUiPort = System.getenv("NETFLOW_QUERY_MASTER_WEBUI_PORT").toInt
  }

  parse(args.toList)

  // This mutates the NetFlowConf, so all accesses to it must be made after this line
  propertiesFile = Utils.loadDefaultProperties(conf, propertiesFile)

  if (conf.contains("netflow.master.ui.port")) {
    webUiPort = conf.get("netflow.master.ui.port").toInt
  }

  def parse(args: List[String]): Unit = args match {
    case ("--host" | "-h") :: value :: tail =>
      Utils.checkHost(value, "Please use hostname " + value)
      host = value
      parse(tail)

    case ("--port" | "-p") :: IntParam(value) :: tail =>
      port = value
      parse(tail)

    case "--webui-port" :: IntParam(value) :: tail =>
      webUiPort = value
      parse(tail)

    case ("--properties-file") :: value :: tail =>
      propertiesFile = value
      parse(tail)

    case ("--help") :: tail =>
      printUsageAndExit(0)

    case Nil => {}

    case _ =>
      printUsageAndExit(1)
  }

  /**
   * Print usage and exit JVM with the given exit code.
   */
  def printUsageAndExit(exitCode: Int) {
    System.err.println(
      "Usage: QueryMaster [options]\n" +
        "\n" +
        "Options:\n" +
        "  -h HOST, --host HOST   Hostname to listen on\n" +
        "  -p PORT, --port PORT   Port to listen on (default: 9099)\n" +
        "  --webui-port PORT      Port for web UI (default: 18080)\n" +
        "  --properties-file FILE Path to a custom NetFlow properties file.\n" +
        "                         Default is conf/netflow-defaults.conf.")
    System.exit(exitCode)
  }

}
