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
package cn.ac.ict.acs.netflow.deploy

import akka.actor.{Props, ActorSystem, Actor}

import cn.ac.ict.acs.netflow.{NetFlowConf, Logging}
import cn.ac.ict.acs.netflow.deploy.DeployMessages._
import cn.ac.ict.acs.netflow.util.{Utils, AkkaUtils, SignalLogger, ActorLogReceive}

class QueryWorker(
    host: String,
    port: Int,
    webUiPort: Int,
    cores: Int,
    memory: Int,
    masterAkkaUrls: Array[String],
    actorSystemName: String,
    actorName: String,
    workDirPath: String = null,
    val conf: NetFlowConf)
  extends Actor with ActorLogReceive with Logging {

  import context.dispatcher

  Utils.checkHost(host, "Expected hostname")
  assert (port > 0)

  def receiveWithLogging = {
    case RegisteredWorker =>

    case RegisterWorkerFailed =>

    case ReconnectWorker =>
  }
}

object QueryWorker extends Logging {
  val systemName = "netflowQueryWorker"
  val actorName = "QueryWorker"

  def main(argStrings: Array[String]) {
    SignalLogger.register(log)
    val conf = new NetFlowConf
    val args = new QueryWorkerArguments(argStrings, conf)
    val (actorSystem, _) = startSystemAndActor(args.host, args.port, args.webUiPort,
      args.cores, args.memory, args.masters, args.workDir, conf = conf)
    actorSystem.awaitTermination()
  }

  def startSystemAndActor(
      host: String,
      port: Int,
      webUiPort: Int,
      cores: Int,
      memory: Int,
      masterUrls: Array[String],
      workDir: String,
      workerNumber: Option[Int] = None,
      conf: NetFlowConf = new NetFlowConf): (ActorSystem, Int) = {

    val sysName = systemName + workerNumber.map(_.toString).getOrElse("")
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(sysName, host, port, conf)
    val masterAkkaUrls = masterUrls.map(
      QueryMaster.toAkkaUrl(_, AkkaUtils.protocol(actorSystem)))
    actorSystem.actorOf(Props(classOf[QueryWorker], host, boundPort, webUiPort, cores,
      memory, masterAkkaUrls, sysName, actorName, workDir, conf), name = actorName)
    (actorSystem, boundPort)
  }
}
