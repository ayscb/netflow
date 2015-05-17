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

import java.util.concurrent.Executors

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

import akka.actor._
import akka.pattern.ask
import akka.serialization.SerializationExtension
import akka.remote.{DisassociatedEvent, RemotingLifecycleEvent}

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import cn.ac.ict.acs.netflow._
import cn.ac.ict.acs.netflow.deploy._
import cn.ac.ict.acs.netflow.deploy.DeployMessages._
import cn.ac.ict.acs.netflow.deploy.RestMessages._
import cn.ac.ict.acs.netflow.deploy.qmaster.QueryMasterMessages._
import cn.ac.ict.acs.netflow.util._

class QueryMaster(
    host: String,
    port: Int,
    webUiPort: Int,
    val conf: NetFlowConf)
  extends Actor with ActorLogReceive with LeaderElectable with Logging {

  import context.dispatcher

  val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(20))

  val createTimeFormat = DateTimeFormat.forPattern("yyyyMMddHHmmss")
  val BROKER_TIMEOUT = conf.getLong("netflow.broker.timeout", 60) * 1000
  val RETAINED_QUERY = conf.getInt("netflow.deploy.retainedQuerys", 200)

  //TODO find right place to set spark configurations used by netflow
  val sparkMasterUrl = conf.get("netflow.spark.master")
  val SPARK_HOME = System.getenv("SPARK_HOME")
  require(SPARK_HOME != null)

  val sparkSubmitPath = SPARK_HOME + "/bin/spark-submit"

  // Remove a dead broker after given interval
  val REAPER_ITERATIONS = conf.getInt("netflow.dead.broker.persistence", 15)
  // Master recovery mode
  val RECOVERY_MODE = conf.get("netflow.deploy.recoveryMode", "NONE")

  // This may contain dead workers
  val brokers = new mutable.HashSet[BrokerInfo]
  // Current alive workers
  val idToBroker = new mutable.HashMap[String, BrokerInfo]
  // Current alive workers
  val addressToBroker = new mutable.HashMap[Address, BrokerInfo]

  // Registered Job query may be scheduled and cancel later
  val idToJob = new mutable.HashMap[String, Cancellable]

  val queries = new mutable.HashSet[QueryInfo]
  val completeQueries = new mutable.ArrayBuffer[QueryInfo]
  val waitingQueries = new mutable.ArrayBuffer[QueryInfo]
  val jobIdToQueries = new mutable.HashMap[String, QueryInfo]

  Utils.checkHost(host, "Expected hostname")

  var nextQueryNumber = 0
  var nextJobNumber = 0

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

    context.system.scheduler.schedule(0.millis, BROKER_TIMEOUT.millis, self, CheckForBrokerTimeOut)

    val (persistenceEngine_, leaderElectionAgent_) = RECOVERY_MODE match {
      case "ZOOKEEPER" =>
        logInfo("Persisting recovery state to ZooKeeper")
        val zkFactory =
          new ZooKeeperRecoveryModeFactory(conf, SerializationExtension(context.system))
        (zkFactory.createPersistenceEngine(), zkFactory.createLeaderElectionAgent(this))
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
    persistenceEngine.close()
    leaderElectionAgent.stop()
    idToJob.values.foreach(_.cancel())
  }

  override def appointLeader() = {
    self ! AppointedAsLeader
  }

  override def revokeLeadership() = {
    self ! RevokedLeadership
  }

  def beginRecovery(
    storedJobs: Seq[Job],
    storedBrokers: Seq[BrokerInfo]): Unit = {

    for (job <- storedJobs) {
      logInfo(s"Trying to recover job: ${job.id}")
      registerInScheduler(job)
    }

    for (broker <- storedBrokers) {
      logInfo("Trying to recover broker: " + broker.id)
      try {
        registerBroker(broker)
        broker.state = BrokerState.UNKNOWN
        broker.actor ! MasterChanged(queryMasterUrl, queryMasterWebUIUrl)
      } catch {
        case e: Exception => logInfo("Broker " + broker.id + " had exception on reconnect")
      }
    }
  }

  def canCompleteRecovery =
    brokers.count(_.state == BrokerState.UNKNOWN) == 0

  def completeRecovery(): Unit = {
    // Ensure "only-once" recovery semantics using a short synchronization period.
    synchronized {
      if (state != QueryMasterRecoveryState.RECOVERING) {
        return
      }
      state = QueryMasterRecoveryState.COMPLETING_RECOVERY
    }
    brokers.filter(_.state == BrokerState.UNKNOWN).foreach(removeBroker)
    state = QueryMasterRecoveryState.ALIVE
    logInfo("Recovery complete - resuming operations!")
  }

  def registerBroker(broker: BrokerInfo): Boolean = {
    // There may be one or more refs to dead brokers on this same node (with different ID's),
    // remove them.
    brokers.filter { b =>
      (b.host == broker.host && b.port == broker.port) && (b.state == BrokerState.DEAD)
    }.foreach { w =>
      brokers -= w
    }

    val brokerAddress = broker.actor.path.address
    if (addressToBroker.contains(brokerAddress)) {
      val oldBroker = addressToBroker(brokerAddress)
      if (oldBroker.state == BrokerState.UNKNOWN) {
        // A worker registering from UNKNOWN implies that the worker was restarted during recovery.
        // The old worker must thus be dead, so we will remove it and accept the new worker.
        removeBroker(oldBroker)
      } else {
        logInfo("Attempted to re-register worker at same address: " + brokerAddress)
        return false
      }
    }

    brokers += broker
    idToBroker(broker.id) = broker
    addressToBroker(brokerAddress) = broker
    true
  }

  def removeBroker(broker: BrokerInfo): Unit = {
    logInfo("Removing broker " + broker.id + " on " + broker.host + ":" + broker.port)
    broker.setState(BrokerState.DEAD)
    idToBroker -= broker.id
    addressToBroker -= broker.actor.path.address
    persistenceEngine.removeBroker(broker)
  }

  /** Check for, and remove, any timed-out brokers */
  def timeOutDeadBrokers() {
    // Copy the workers into an array so we don't modify the hashset while iterating through it
    val currentTime = System.currentTimeMillis()
    val toRemove = brokers.filter(_.lastHeartbeat < currentTime - BROKER_TIMEOUT).toArray
    for (broker <- toRemove) {
      if (broker.state != BrokerState.DEAD) {
        logWarning("Removing %s because we got no heartbeat in %d seconds".format(
          broker.id, BROKER_TIMEOUT / 1000))
        removeBroker(broker)
      } else {
        if (broker.lastHeartbeat < currentTime - ((REAPER_ITERATIONS + 1) * BROKER_TIMEOUT)) {
          brokers -= broker // we've seen this DEAD worker in the UI, etc. for long enough; cull it
        }
      }
    }
  }

  override def receiveWithLogging = {
    case AppointedAsLeader => {
      val (storedJobs, storedBroker) = persistenceEngine.readPersistedData()
      state = if (storedJobs.isEmpty) {
        QueryMasterRecoveryState.ALIVE
      } else {
        QueryMasterRecoveryState.RECOVERING
      }
      logInfo("I have been elected leader! New state: " + state)
      if (state == QueryMasterRecoveryState.RECOVERING) {
        beginRecovery(storedJobs, storedBroker)
        recoveryCompletionTask = context.system.scheduler.scheduleOnce(BROKER_TIMEOUT.millis, self,
          CompleteRecovery)
      }
    }

    case CompleteRecovery =>
      completeRecovery()

    case BrokerStateResponse(brokerId) => {
      idToBroker.get(brokerId) match {
        case Some(broker) =>
          logInfo("broker has been re-registered: " + brokerId)
          broker.state = BrokerState.ALIVE

        case None =>
          logWarning(s"Scheduler state from unknown broker: ${brokerId}")
      }

      if (canCompleteRecovery) {
        completeRecovery()
      }
    }

    case RevokedLeadership => {
      logError("Leadership has been revoked -- query master shutting down.")
      System.exit(0)
    }

    case RegisterBroker(id, host, port, restPort) => {
      logInfo("Registering broker %s:%d".format(host, port))
      if (state == QueryMasterRecoveryState.STANDBY) {
        // ignore, don't send response
      } else if (idToBroker.contains(id)) {
        sender ! RegisterBrokerFailed("Duplicate worker ID")
      } else {
        val broker = new BrokerInfo(id, host, port, restPort, sender)
        if (registerBroker(broker)) {
          persistenceEngine.addBroker(broker)
          sender ! RegisteredBroker(queryMasterUrl, queryMasterWebUIUrl)
        } else {
          val brokerAddress = broker.actor.path.address
          logWarning("Broker registration failed. Attempted to re-register worker at same " +
            "address: " + brokerAddress)
          sender ! RegisterBrokerFailed("Attempted to re-register worker at same address: "
            + brokerAddress)
        }
      }
    }

    case Heartbeat(brokerId) => {
      idToBroker.get(brokerId) match {
        case Some(broker) =>
          broker.lastHeartbeat = System.currentTimeMillis()
        case None => {
          if (brokers.map(_.id).contains(brokerId)) {
            logWarning(s"Got heartbeat from unregistered broker $brokerId." +
              " Asking it to re-register.")
            sender ! ReconnectBroker(queryMasterUrl)
          } else {
            logWarning(s"Got heartbeat from unregistered worker $brokerId." +
              " This worker was never registered, so ignoring the heartbeat.")
          }
        }
      }
    }

    case RestRequestQueryMasterStatus => {

    }

    case RestRequestAllJobsInfo => {

    }

    case RestRequestJobInfo(jobId) => {

    }

    case CheckForBrokerTimeOut => {
      timeOutDeadBrokers()
    }

    case RestRequestSubmitJob(jobDesc) => {
      if (state != QueryMasterRecoveryState.ALIVE) {
        val msg = s"Can only accept job registration in ALIVE state. Current state: $state."
        sender ! RestRequestFailed(msg)
      } else {
        logInfo(s"New Job registered, Info: [type: ${tpe}, " +
          s"query: ${cmd.appName}]")
        val job = createJob(tpe, firstShot, interval, cmd)
        persistenceEngine.addJob(job)
        registerInScheduler(job)
        sender ! RestResponseSubmitJobSuccess(job.id,
          s"Job successfully submitted as ${job.id}")
      }
    }

    case RestRequestKillJob(jobId) => {
      if (state != QueryMasterRecoveryState.ALIVE) {
        val msg = s"Can only cancel job in ALIVE state. Current state: $state."
        sender ! RestRequestFailed(msg)
      } else {
        logInfo(s"Asked to cancel job ${jobId}")
        persistenceEngine.removeJob(jobId)
        jobIdToQueries.get(jobId) match {
          case Some(q) =>
            self ! KillQuery(q.id)
          case None => //do nothing
        }
      }
    }

    case SubmitQuery(jobId, tpe, cmd) => {
      if (state != QueryMasterRecoveryState.ALIVE) {
        // do nothing since we are not master
      } else {
        logInfo(s"Query for Job ${jobId} submitted: ${cmd.appName}")

        // adhoc or online job is scheduled once
        // so we should remove it from PE once it get scheduled
        if (tpe == JobType.ADHOC || tpe == JobType.ONLINE) {
          persistenceEngine.removeJob(jobId)
        }

        val query = createQuery(jobId, cmd)

        val submitFuture = concurrent.future {
          val pb = new ProcessBuilder(sparkSubmitPath, cmd.toString())
          pb.start()
        }(ec)
        submitFuture.failed.foreach {e =>
          logError(s"Submit Query ${cmd.appName} failed: ${e.getMessage}", e)
        }(ec)
      }
    }

    case KillQuery(queryId) => {
      if (state != QueryMasterRecoveryState.ALIVE) {
        val msg = s"Can only kill queries in ALIVE state, current state is $state"
        sender ! KillQueryResponse(queryId, success = false, msg)
      } else {
      }
    }

    case BoundPortsRequest =>
      sender ! BoundPortsResponse(port, webUiPort)

    case DisassociatedEvent(_, address, _) => {
      // The disconnected client could've been either a worker or an app; remove whichever it was
      logInfo(s"$address got disassociated, removing it.")
      addressToBroker.get(address).foreach(removeBroker)
      if (state == QueryMasterRecoveryState.RECOVERING && canCompleteRecovery) {
        completeRecovery()
      }
    }
  }

  def createQuery(jobId: String, cmd: Command): QueryInfo = {
    def newQueryId(submitDate: DateTime): String = {
      val queryId = "query-%s-%04d".format(
        submitDate.toString(createTimeFormat),
        nextQueryNumber)
      nextQueryNumber += 1
      queryId
    }

    val now = System.currentTimeMillis()
    val date = new DateTime(now)
    new QueryInfo(jobId, newQueryId(date), now, cmd, date)
  }

  def createJob(
      tpe: JobType.Value,
      firstShot: Long,
      interval: Option[Long],
      cmd: Command): Job = {
    def newJobId(submitDate: DateTime, tpe: JobType.Value): String = {
      val jobId = "job-%s-%s-%6d".format(
        tpe.toString.toLowerCase,
        submitDate.toString(createTimeFormat),
        nextJobNumber)
      nextJobNumber += 1
      jobId
    }

    val now = System.currentTimeMillis()
    val date = new DateTime(now)
    Job(newJobId(date, tpe), tpe, firstShot, interval, cmd)
  }

  // If didn't pass delay, get remaining time, else 0
  def delayOrZero(initialDelay: Long): Long = {
    math.max(initialDelay - System.currentTimeMillis(), 0)
  }

  def delayRound(initialDelay: Long, interval: Long): Long = {
    val now = System.currentTimeMillis()
    if (now < initialDelay) {
      initialDelay - now
    } else {
      now - initialDelay - ((now - initialDelay) / interval) * interval
    }
  }

  def registerInScheduler(job: Job): Unit = {
    val timerTask = job.tpe match {
      case JobType.ADHOC | JobType.ONLINE =>
        val delay = delayOrZero(job.first)
        context.system.scheduler.scheduleOnce(
          delay.millis, self, SubmitQuery(job.id, job.tpe, job.cmd))
      case JobType.REPORT =>
        val delay = delayRound(job.first, job.interval.get)
        context.system.scheduler.schedule(
          delay.millis, job.interval.get.millis,
          self, SubmitQuery(job.id, job.tpe, job.cmd))
    }
    idToJob(job.id) = timerTask
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

  /**
   * Returns an `akka.tcp://...` URL for the Master actor given a
   * netflowkUrl `netflow-query://host:port`.
   *
   * @throws NetFlowException if the url is invalid
   */
  def toAkkaUrl(netflowUrl: String, protocol: String): String = {
    val (host, port) = Utils.extractHostPortFromNetFlowUrl(netflowUrl)
    AkkaUtils.address(protocol, systemName, host, port, actorName)
  }

  /**
   * Returns an akka `Address` for the Master actor given a
   * netflowkUrl `netflow-query://host:port`.
   *
   * @throws NetFlowException if the url is invalid
   */
  def toAkkaAddress(netflowUrl: String, protocol: String): Address = {
    val (host, port) = Utils.extractHostPortFromNetFlowUrl(netflowUrl)
    Address(protocol, systemName, host, port)
  }
}
