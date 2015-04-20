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

import akka.remote.RemotingLifecycleEvent
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor._
import akka.pattern.ask

import cn.ac.ict.acs.netflow.{QueryDescription, NetFlowConf, Logging}
import cn.ac.ict.acs.netflow.deploy.MasterMessages._
import cn.ac.ict.acs.netflow.deploy.DeployMessages._
import cn.ac.ict.acs.netflow.deploy.Messages._
import cn.ac.ict.acs.netflow.util._

class QueryMaster(
    host: String,
    port: Int,
    webUiPort: Int,
    val conf: NetFlowConf)
  extends Actor with ActorLogReceive with LeaderElectable with Logging {

  import context.dispatcher

  val createTimeFormat = DateTimeFormat.forPattern("yyyyMMddHHmmss")
  val WORKER_TIMEOUT = conf.getLong("netflow.queryWorker.timeout", 60) * 1000
  val RETAINED_QUERY = conf.getInt("netflow.deploy.retainedQuerys", 200)

  // Remove a dead worker after given interval
  val REAPER_ITERATIONS = conf.getInt("netflow.dead.worker.persistence", 15)
  // Master recovery mode
  val RECOVERY_MODE = conf.get("netflow.deploy.recoveryMode", "NONE")

  val workers = new HashSet[WorkerInfo]
  val idToWorker = new HashMap[String, WorkerInfo]
  val addressToWorker = new HashMap[Address, WorkerInfo]

  val queries = new HashSet[QueryDescription]
  val adhocQueries = new HashSet[QueryDescription]
  val reportQueries = new HashSet[QueryDescription]
  val onlineQueries = new HashSet[QueryDescription]
  var nextQueryNumber = 0

  Utils.checkHost(host, "Expected hostname")

  val queryMasterUrl = "netflow://" + host + ":" + port
  var queryMasterWebUIUrl: String = _

  var persistenceEngine: PersistenceEngine = _

  var leaderElectionAgent: LeaderElectionAgent = _

  private var recoveryCompletionTask: Cancellable = _

  override def preStart(): Unit = {
    logInfo("Starting NetFlow QueryMaster at " + queryMasterUrl)
    logInfo(s"Running NetFlow version ${cn.ac.ict.acs.netflow.NETFLOW_VERSION}")
    // Listen for remote client disconnection events, since they don't go through Akka's watch()
    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
    context.system.scheduler.schedule(0 millis, WORKER_TIMEOUT millis, self, CheckForWorkerTimeOut)

    val (persistenceEngine_, leaderElectionAgent_) = RECOVERY_MODE match {
      case "ZOOKEEPER"  =>
        logInfo("Persisting recovery state to ZooKeeper")
        //TODO ZK Impl here
        (new BlackHolePersistenceEngine(), new MonarchyLeaderAgent(this))
      case _ =>
        logInfo("No state persisted as a MonarchyLeader")
        (new BlackHolePersistenceEngine(), new MonarchyLeaderAgent(this))
    }
    persistenceEngine = persistenceEngine_
    leaderElectionAgent = leaderElectionAgent_
  }

  override def preRestart(reason: Throwable, message: Option[Any]) {
    super.preRestart(reason, message) // calls postStop()!
    logError("QueryMaster actor restarted due to exception", reason)
  }

  override def postStop(): Unit = {
    if (recoveryCompletionTask != null) {
      recoveryCompletionTask.cancel()
    }
    persistenceEngine.close()
    leaderElectionAgent.stop()
  }

  override def appointLeader() = {
    self ! AppointedAsLeader
  }

  override def revokeLeadership() = {
    self ! RevokedLeadership
  }


  def receiveWithLogging = {
    case AppointedAsLeader =>

    case RevokedLeadership =>

    case RegisterWorker(id, host, port, cores, memory, webUiPort, publicAddress) =>

    case Heartbeat(workerId) =>

    case CheckForWorkerTimeOut =>

    case RegisterQuery =>

    case BoundPortsRequest =>
      sender ! BoundPortsResponse(port, webUiPort)
  }

  def createQuery() = ???


}

object QueryMaster extends Logging {
  val systemName = "netflowQueryMaster"
  private val actorName = "QueryMaster"

  def main(argStrings: Array[String]): Unit = {
    SignalLogger.register(log)
    val conf = new NetFlowConf
    val masterArg = new QueryMasterArguments(argStrings, conf)
    val (actorSystem, _, _) =
      startSystemAndActor(masterArg.host, masterArg.port, masterArg.webUiPort, conf)
    actorSystem.awaitTermination()
  }

  /**
   * Start the Master and return a four tuple of:
   * (1) The Master actor system
   * (2) The bound port
   * (3) The web UI bound port
   */
  def startSystemAndActor(
      host: String,
      port: Int,
      webUiPort: Int,
      conf: NetFlowConf): (ActorSystem, Int, Int) = {
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(systemName, host, port, conf)
    val actor = actorSystem.actorOf(
      Props(classOf[QueryMaster], host, boundPort, webUiPort, conf), actorName)
    val timeout = AkkaUtils.askTimeout(conf)
    val portsRequest = actor.ask(BoundPortsRequest)(timeout)
    val portsResponse = Await.result(portsRequest, timeout).asInstanceOf[BoundPortsResponse]
    (actorSystem, boundPort, portsResponse.webUIPort)
  }
}
