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

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._

import akka.remote.RemotingLifecycleEvent
import akka.actor._
import akka.pattern.ask

import org.joda.time.format.DateTimeFormat

import cn.ac.ict.acs.netflow.{NetFlowException, QueryDescription, NetFlowConf, Logging}
import cn.ac.ict.acs.netflow.deploy.QueryMasterMessages._
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

  // This may contain dead workers
  val workers = new mutable.HashSet[QueryWorkerInfo]
  // Current alive workers
  val idToWorker = new mutable.HashMap[String, QueryWorkerInfo]
  // Current alive workers
  val addressToWorker = new mutable.HashMap[Address, QueryWorkerInfo]

  val queries = new mutable.HashSet[QueryDescription]
  val adhocQueries = new mutable.HashSet[QueryDescription]
  val reportQueries = new mutable.HashSet[QueryDescription]
  val onlineQueries = new mutable.HashSet[QueryDescription]
  var nextQueryNumber = 0

  Utils.checkHost(host, "Expected hostname")

  val queryMasterUrl = "netflow-query://" + host + ":" + port
  var queryMasterWebUIUrl: String = _

  var state = QueryMasterRecoveryState.STANDBY

  var persistenceEngine: PersistenceEngine = _

  var leaderElectionAgent: LeaderElectionAgent = _

  private var recoveryCompletionTask: Cancellable = _

  override def preStart(): Unit = {
    logInfo("Starting NetFlow QueryMaster at " + queryMasterUrl)
    logInfo(s"Running NetFlow version ${cn.ac.ict.acs.netflow.NETFLOW_VERSION}")
    // Listen for remote client disconnection events, since they don't go through Akka's watch()
    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])

    //TODO: a pseudo webuiurl here
    queryMasterWebUIUrl = "http://" + host + ":" + webUiPort

    context.system.scheduler.schedule(0.millis, WORKER_TIMEOUT.millis, self, CheckForWorkerTimeOut)

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


  def receiveWithLogging: PartialFunction[Any, Unit] = {
    case AppointedAsLeader =>
      //TODO: dummy placeholder
      state = QueryMasterRecoveryState.ALIVE

    case RevokedLeadership => {
      logError("Leadership has been revoked -- query master shutting down.")
      System.exit(0)
    }

    case RegisterWorker(id, host, port, cores, memory, webUiPort) => {
      logInfo("Registering query worker %s:%d with %d cores, %s RAM".format(
        host, port, cores, Utils.megabytesToString(memory)))
      if (state == QueryMasterRecoveryState.STANDBY) {
        // ignore, don't send response
      } else if (idToWorker.contains(id)) {
        sender ! RegisterWorkerFailed("Duplicate worker ID")
      } else {
        val worker = new QueryWorkerInfo(id, host, port, cores, memory,
          sender, webUiPort)
        if (registerWorker(worker)) {
          persistenceEngine.addWorker(worker)
          sender ! RegisteredWorker(queryMasterUrl, queryMasterWebUIUrl)
          schedule()
        } else {
          val workerAddress = worker.actor.path.address
          logWarning("Worker registration failed. Attempted to re-register worker at same " +
            "address: " + workerAddress)
          sender ! RegisterWorkerFailed("Attempted to re-register worker at same address: "
            + workerAddress)
        }
      }
    }

    case Heartbeat(workerId) =>

      idToWorker.get(workerId) match {
        case Some(workerInfo) =>
          workerInfo.lastHeartbeat = System.currentTimeMillis()
        case None =>
          if (workers.map(_.id).contains(workerId)) {
            logWarning(s"Got heartbeat from unregistered query worker $workerId." +
              " Asking it to re-register.")
            sender ! ReconnectWorker(queryMasterUrl)
          } else {
            logWarning(s"Got heartbeat from unregistered worker $workerId." +
              " This worker was never registered, so ignoring the heartbeat.")
          }
      }

    case CheckForWorkerTimeOut =>
      timeOutDeadWorkers()

    case RegisterQuery(queryId) =>

    case BoundPortsRequest =>
      sender ! BoundPortsResponse(port, webUiPort)
  }

  def createQuery() = ???

  def registerWorker(worker: QueryWorkerInfo): Boolean = {
    // There may be one or more refs to dead workers on this same node (with different ID's),
    // remove them.
    workers.filter { w =>
      (w.host == worker.host && w.port == worker.port) && (w.state == QueryWorkerState.DEAD)
    }.foreach { w =>
      workers -= w
    }

    val workerAddress = worker.actor.path.address
    if (addressToWorker.contains(workerAddress)) {
      val oldWorker = addressToWorker(workerAddress)
      if (oldWorker.state == QueryWorkerState.UNKNOWN) {
        // A worker registering from UNKNOWN implies that the worker was restarted during recovery.
        // The old worker must thus be dead, so we will remove it and accept the new worker.
        removeWorker(oldWorker)
      } else {
        logInfo("Attempted to re-register worker at same address: " + workerAddress)
        return false
      }
    }

    workers += worker
    idToWorker(worker.id) = worker
    addressToWorker(workerAddress) = worker
    true
  }

  def removeWorker(worker: QueryWorkerInfo): Unit = {
    logInfo("Removing worker " + worker.id + " on " + worker.host + ":" + worker.port)
    worker.setState(QueryWorkerState.DEAD)
    idToWorker -= worker.id
    addressToWorker -= worker.actor.path.address

    //TODO: foreach query launched by this worker, also remove it?

    persistenceEngine.removeWorker(worker)
  }

  /**
   * Schedule the currently available resources among waiting queries. This method will be called
   * each time a new query joins or resource availability changes.
   */
  private def schedule(): Unit = {

  }

  /** Check for, and remove, any timed-out workers */
  def timeOutDeadWorkers() {
    // Copy the workers into an array so we don't modify the hashset while iterating through it
    val currentTime = System.currentTimeMillis()
    val toRemove = workers.filter(_.lastHeartbeat < currentTime - WORKER_TIMEOUT).toArray
    for (worker <- toRemove) {
      if (worker.state != QueryWorkerState.DEAD) {
        logWarning("Removing %s because we got no heartbeat in %d seconds".format(
          worker.id, WORKER_TIMEOUT/1000))
        removeWorker(worker)
      } else {
        if (worker.lastHeartbeat < currentTime - ((REAPER_ITERATIONS + 1) * WORKER_TIMEOUT)) {
          workers -= worker // we've seen this DEAD worker in the UI, etc. for long enough; cull it
        }
      }
    }
  }

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
   * Returns an `akka.tcp://...` URL for the Master actor given a
   * netflowkUrl `netflow-query://host:port`.
   *
   * @throws NetFlowException if the url is invalid
   */
  def toAkkaUrl(sparkUrl: String, protocol: String): String = {
    val (host, port) = Utils.extractHostPortFromSparkUrl(sparkUrl)
    AkkaUtils.address(protocol, systemName, host, port, actorName)
  }

  /**
   * Returns an akka `Address` for the Master actor given a
   * netflowkUrl `netflow-query://host:port`.
   *
   * @throws NetFlowException if the url is invalid
   */
  def toAkkaAddress(sparkUrl: String, protocol: String): Address = {
    val (host, port) = Utils.extractHostPortFromSparkUrl(sparkUrl)
    Address(protocol, systemName, host, port)
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
