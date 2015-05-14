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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.ac.ict.acs.netflow.load2.deploy.LoadDeploy

import java.lang.management.ManagementFactory

import cn.ac.ict.acs.netflow.NetFlowConf
import cn.ac.ict.acs.netflow.util.{IntParam, MemoryParam, Utils}

/**
 * Command-line parser for the worker.
 * copy from Spark
 * Created by ayscb on 2015/4/28.
 */
class LoadWorkerArguments (args: Array[String], conf: NetFlowConf){

    var host = Utils.localHostName()
    var port = 0
    var webUiPort = 18081
    var cores = inferDefaultCores()
    var memory = inferDefaultMemory()
    var masters: Array[String] = _
    var workDir: String = _
    var propertiesFile: String = _

    // Check for settings in environment variables
    if (System.getenv("NETFLOW_QUERY_WORKER_PORT") != null) {
      port = System.getenv("NETFLOW_QUERY_WORKER_PORT").toInt
    }
    if (System.getenv("NETFLOW_QUERY_WORKER_CORES") != null) {
      cores = System.getenv("NETFLOW_QUERY_WORKER_CORES").toInt
    }
    if (System.getenv("NETFLOW_QUERY_WORKER_MEMORY") != null) {
      memory = Utils.memoryStringToMb(System.getenv("NETFLOW_QUERY_WORKER_MEMORY"))
    }
    if (System.getenv("NETFLOW_QUERY_WORKER_WEBUI_PORT") != null) {
      webUiPort = System.getenv("NETFLOW_QUERY_WORKER_WEBUI_PORT").toInt
    }
    if (System.getenv("NETFLOW_QUERY_WORKER_DIR") != null) {
      workDir = System.getenv("NETFLOW_QUERY_WORKER_DIR")
    }

    parse(args.toList)

    propertiesFile = Utils.loadDefaultProperties(conf, propertiesFile)

    checkWorkerMemory()

    private def parse(args: List[String]): Unit = args match {
      case ("--host" | "-h") :: value :: tail =>
        Utils.checkHost(value, "Please use hostname " + value)
        host = value
        parse(tail)

      case ("--port" | "-p") :: IntParam(value) :: tail =>
        port = value
        parse(tail)

      case ("--cores" | "-c") :: IntParam(value) :: tail =>
        cores = value
        parse(tail)

      case ("--memory" | "-m") :: MemoryParam(value) :: tail =>
        memory = value
        parse(tail)

      case ("--work-dir" | "-d") :: value :: tail =>
        workDir = value
        parse(tail)

      case "--webui-port" :: IntParam(value) :: tail =>
        webUiPort = value
        parse(tail)

      case ("--properties-file") :: value :: tail =>
        propertiesFile = value
        parse(tail)

      case ("--help") :: tail =>
        printUsageAndExit(0)

      case value :: tail =>
        if (masters != null) {  // Two positional arguments were given
          printUsageAndExit(1)
        }
        masters = value.stripPrefix("netflow-query://").split(",").map("netflow-query://" + _)
        parse(tail)

      case Nil =>
        if (masters == null) {  // No positional argument was given
          printUsageAndExit(1)
        }

      case _ =>
        printUsageAndExit(1)
    }

    /**
      * Print usage and exit JVM with the given exit code.
     */
    private def printUsageAndExit(exitCode: Int) {
      System.err.println(
        "Usage: QueryWorker [options] <master>\n" +
          "\n" +
          "Master must be a URL of the form netflow-query://hostname:port\n" +
          "\n" +
          "Options:\n" +
          "  -c CORES, --cores CORES  Number of cores to use\n" +
          "  -m MEM, --memory MEM     Amount of memory to use (e.g. 1000M, 2G)\n" +
          "  -d DIR, --work-dir DIR   Directory to run apps in (default: NETFLOW_HOME/work)\n" +
          "  -h HOST, --host HOST     Hostname to listen on\n" +
          "  -p PORT, --port PORT     Port to listen on (default: random)\n" +
          "  --webui-port PORT        Port for web UI (default: 18081)\n" +
          "  --properties-file FILE   Path to a custom netflow properties file.\n" +
          "                           Default is conf/netflow-defaults.conf.")
      System.exit(exitCode)
    }

    def inferDefaultCores(): Int = {
      Runtime.getRuntime.availableProcessors()
    }

    def inferDefaultMemory(): Int = {
      val ibmVendor = System.getProperty("java.vendor").contains("IBM")
      var totalMb = 0
      try {
        val bean = ManagementFactory.getOperatingSystemMXBean()
        if (ibmVendor) {
          val beanClass = Class.forName("com.ibm.lang.management.OperatingSystemMXBean")
          val method = beanClass.getDeclaredMethod("getTotalPhysicalMemory")
          totalMb = (method.invoke(bean).asInstanceOf[Long] / 1024 / 1024).toInt
        } else {
          val beanClass = Class.forName("com.sun.management.OperatingSystemMXBean")
          val method = beanClass.getDeclaredMethod("getTotalPhysicalMemorySize")
          totalMb = (method.invoke(bean).asInstanceOf[Long] / 1024 / 1024).toInt
        }
      } catch {
        case e: Exception => {
          totalMb = 2 * 1024
          System.out.println("Failed to get total physical memory. Using " + totalMb + " MB")
        }
      }
      // Leave out 1 GB for the operating system, but don't return a negative memory size
      math.max(totalMb - 1024, 512)
    }

    private def checkWorkerMemory(): Unit = {
      if (memory <= 0) {
        val message = "Memory can't be 0, missing a M or G on the end of the memory specification?"
        throw new IllegalStateException(message)
      }
    }
  }
