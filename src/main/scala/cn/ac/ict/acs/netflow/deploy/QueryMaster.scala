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
import scala.util.Random

import akka.actor._
import akka.remote.{DisassociatedEvent, RemotingLifecycleEvent}
import akka.pattern.ask
import akka.serialization.SerializationExtension

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import cn.ac.ict.acs.netflow.{NetFlowException, NetFlowConf, Logging}
import cn.ac.ict.acs.netflow.deploy.QueryMasterMessages._
import cn.ac.ict.acs.netflow.deploy.DeployMessages._
import cn.ac.ict.acs.netflow.deploy.Messages._
import cn.ac.ict.acs.netflow.deploy.QueryState.QueryState
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

    context.system.scheduler.schedule(0.millis, WORKER_TIMEOUT.millis, self, CheckForWorkerTimeOut)

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
    if (recoveryCompletionTask != null) {
      recoveryCompletionTask.cancel()
    }
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

  override def receiveWithLogging = {
    case AppointedAsLeader => {
      val (storedJobs, storedQueries, storedWorkers) = persistenceEngine.readPersistedData()
      state = if (storedQueries.isEmpty && storedWorkers.isEmpty) {
        QueryMasterRecoveryState.ALIVE
      } else {
        QueryMasterRecoveryState.RECOVERING
      }
      logInfo("I have been elected leader! New state: " + state)
      if (state == QueryMasterRecoveryState.RECOVERING) {
        beginRecovery(storedJobs, storedQueries, storedWorkers)
        recoveryCompletionTask = context.system.scheduler.scheduleOnce(WORKER_TIMEOUT.millis, self,
          CompleteRecovery)
      }
    }

    case CompleteRecovery =>
      completeRecovery()

    case WorkerSchedulerStateResponse(workerId, queryIds) => {
      idToWorker.get(workerId) match {
        case Some(worker) =>
          logInfo("Worker has been re-registered: " + workerId)
          worker.state = QueryWorkerState.ALIVE

          //recover queries
          for (queryId <- queryIds) {
            queries.find(_.id == queryId).foreach { query =>
              query.worker = Some(worker)
              query.state = QueryState.RUNNING
              worker.addQuery(query)
            }
          }

        case None =>
          logWarning(s"Scheduler state from unknown worker: ${workerId}")
      }

      if (canCompleteRecovery) {
        completeRecovery()
      }
    }

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

    case RegisterJob(tpe, firstShot, interval, desc) => {
      if (state != QueryMasterRecoveryState.ALIVE) {
        val msg = s"Can only accept job registration in ALIVE state. Current state: $state."
        sender ! RegisterJobResponse(false, None, msg)
      } else {
        logInfo(s"New Job registered, Info: [type: ${tpe}, " +
          s"query: ${desc.command.queryString}]")
        val job = createJob(tpe, firstShot, interval, desc)
        persistenceEngine.addJob(job)
        registerInScheduler(job)
        sender ! RegisterJobResponse(true, Some(job.id),
          s"Job successfully submitted as ${job.id}")
      }
    }

    case CancelJob(jobId) => {
      if (state != QueryMasterRecoveryState.ALIVE) {
        val msg = s"Can only cancel job in ALIVE state. Current state: $state."
        sender ! CancelJobResponse(jobId, false, msg)
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

    case SubmitQuery(jobId, tpe, queryDesc) => {
      if (state != QueryMasterRecoveryState.ALIVE) {
        // do nothing since we are not master
      } else {
        logInfo(s"Query for Job ${jobId} submitted: ${queryDesc.command.mainClass}")

        // adhoc or online job is scheduled once
        // so we should remove it from PE once it get scheduled
        if (tpe == JobType.ADHOC || tpe == JobType.ONLINE) {
          persistenceEngine.removeJob(jobId)
        }

        val query = createQuery(jobId, queryDesc)
        persistenceEngine.addQuery(query)
        waitingQueries += query
        queries.add(query)
        jobIdToQueries(jobId) = query
        schedule()
      }
      //self ! TODO should I reply to myself that query has submitted successfully?
    }

    case KillQuery(queryId) => {
      if (state != QueryMasterRecoveryState.ALIVE) {
        val msg = s"Can only kill queries in ALIVE state, current state is $state"
        sender ! KillQueryResponse(queryId, success = false, msg)
      } else {
        logInfo(s"Asked to kill query $queryId")
        val query = queries.find(_.id == queryId)
        query match {
          case Some(q) => {
            if (waitingQueries.contains(q)) {
              waitingQueries -= q
              self ! QueryStateChange(queryId, QueryState.KILLED, None)
            } else {
              // We just notify the worker to kill the query here. The final bookkeeping occurs
              // on the return path when the worker submits a state change back to the master
              // to notify it that the query was successfully killed.
              q.worker.foreach { worker =>
                worker.actor ! KillQuery(queryId)
              }
            }
          }

          case None => {
            val msg = s"Query $queryId has already finished or does not exist"
            logWarning(msg)
            sender ! KillQueryResponse(queryId, success = false, msg)
          }
        }
      }
    }

    case QueryStateChange(queryId, state, exception) => {
      state match {
        case QueryState.ERROR | QueryState.FINISHED | QueryState.KILLED | QueryState.FAILED =>
          removeQuery(queryId, state, None)
        case _ =>
          throw new Exception(s"Received unexpected state for query $queryId: $state")
      }
    }

    case Heartbeat(workerId) => {
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
    }

    case CheckForWorkerTimeOut =>
      timeOutDeadWorkers()

    case BoundPortsRequest =>
      sender ! BoundPortsResponse(port, webUiPort)

    case DisassociatedEvent(_, address, _) => {
      // The disconnected client could've been either a worker or an app; remove whichever it was
      logInfo(s"$address got disassociated, removing it.")
      addressToWorker.get(address).foreach(removeWorker)
      //      addressToApp.get(address).foreach(finishApplication)
      if (state == QueryMasterRecoveryState.RECOVERING && canCompleteRecovery) {
        completeRecovery()
      }
    }
  }

  def beginRecovery(
      storedJobs: Seq[Job],
      storedQueries: Seq[QueryInfo],
      storedWorkers: Seq[QueryWorkerInfo]): Unit = {

    for (job <- storedJobs) {
      logInfo(s"Trying to recover job: ${job.id}")
      registerInScheduler(job)
    }

    for (query <- storedQueries) {
      logInfo("Trying to recover query: " + query.id)
      try {
        queries += query
      } catch {
        case e: Exception => logInfo("Query " + query.id + " had exception")
      }
    }

    for (worker <- storedWorkers) {
      logInfo("Trying to recover worker: " + worker.id)
      try {
        registerWorker(worker)
        worker.state = QueryWorkerState.UNKNOWN
        worker.actor ! MasterChanged(queryMasterUrl, queryMasterWebUIUrl)
      } catch {
        case e: Exception => logInfo("Worker " + worker.id + " had exception on reconnect")
      }
    }
  }

  def relaunchQuery(q: QueryInfo) = {
    q.worker = None
    q.state = QueryState.RELAUNCHING
    waitingQueries += q
    schedule()
  }

  def removeQuery(queryId: String, finalState: QueryState, exception: Option[Exception]) = {
    queries.find(_.id == queryId) match {
      case Some(q) => {
        logInfo(s"Rmoving query: $queryId")
        queries -= q
        if (completeQueries.size >= RETAINED_QUERY) {
          val toRemove = math.max(RETAINED_QUERY / 10, 1)
          completeQueries.trimStart(toRemove)
        }
        completeQueries += q
        persistenceEngine.removeQuery(q)
        q.state = finalState
        q.exception = exception
        q.worker.foreach(w => w.removeQuery(q))
        schedule()
      }
      case None =>
        logWarning(s"Asked to remove unknown query: $queryId")
    }
  }

  def completeRecovery(): Unit = {
    // Ensure "only-once" recovery semantics using a short synchronization period.
    synchronized {
      if (state != QueryMasterRecoveryState.RECOVERING) {
        return
      }
      state = QueryMasterRecoveryState.COMPLETING_RECOVERY
    }

    workers.filter(_.state == QueryWorkerState.UNKNOWN).foreach(removeWorker)

    queries.filter(_.worker.isEmpty).foreach { q =>
      logWarning(s"Query ${q.id} was not found after master recovery")
      if (q.desc.supervise) {
        logWarning(s"Re-launching ${q.id}")
        relaunchQuery(q)
      } else {
        removeQuery(q.id, QueryState.ERROR, None)
        logWarning(s"Did not re-launch ${q.id} because it was not supervised")
      }
    }

    state = QueryMasterRecoveryState.ALIVE
    schedule()
    logInfo("Recovery complete - resuming operations!")
  }

  def createQuery(jobId: String, desc: QueryDescription): QueryInfo = {
    def newQueryId(submitDate: DateTime): String = {
      val queryId = "query-%s-%04d".format(
        submitDate.toString(createTimeFormat),
        nextQueryNumber)
      nextQueryNumber += 1
      queryId
    }

    val now = System.currentTimeMillis()
    val date = new DateTime(now)
    new QueryInfo(jobId, newQueryId(date), now, desc, date)
  }

  def createJob(
      tpe: JobType.Value,
      firstShot: Long,
      interval: Option[Long],
      desc: QueryDescription): Job = {
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
    Job(newJobId(date, tpe), tpe, firstShot, interval, desc)
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
          delay.millis, self, SubmitQuery(job.id, job.tpe, job.desc))
      case JobType.REPORT =>
        val delay = delayRound(job.first, job.interval.get)
        context.system.scheduler.schedule(
          delay.millis, job.interval.get.millis,
          self, SubmitQuery(job.id, job.tpe, job.desc))
    }
    idToJob(job.id) = timerTask
  }

  def canCompleteRecovery =
    workers.count(_.state == QueryWorkerState.UNKNOWN) == 0
  //      apps.count(_.state == ApplicationState.UNKNOWN) == 0

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

  /** Check for, and remove, any timed-out workers */
  def timeOutDeadWorkers() {
    // Copy the workers into an array so we don't modify the hashset while iterating through it
    val currentTime = System.currentTimeMillis()
    val toRemove = workers.filter(_.lastHeartbeat < currentTime - WORKER_TIMEOUT).toArray
    for (worker <- toRemove) {
      if (worker.state != QueryWorkerState.DEAD) {
        logWarning("Removing %s because we got no heartbeat in %d seconds".format(
          worker.id, WORKER_TIMEOUT / 1000))
        removeWorker(worker)
      } else {
        if (worker.lastHeartbeat < currentTime - ((REAPER_ITERATIONS + 1) * WORKER_TIMEOUT)) {
          workers -= worker // we've seen this DEAD worker in the UI, etc. for long enough; cull it
        }
      }
    }
  }

  def launchQuery(worker: QueryWorkerInfo, query: QueryInfo) = {
    logInfo(s"Launching query ${query.id} on worker ${worker.id}")
    worker.addQuery(query)
    query.worker = Some(worker)
    worker.actor ! LaunchQuery(query.id, query.desc)
    query.state = QueryState.RUNNING
  }

  /**
   * Schedule the currently available resources among waiting queries. This method will be called
   * each time a new query joins or resource availability changes.
   */
  def schedule(): Unit = {
    if (state != QueryMasterRecoveryState.ALIVE) { return }

    // Balance Queries by randomization
    val shuffledAliveWorkers = Random.shuffle(
      workers.toSeq.filter(_.state == QueryWorkerState.ALIVE))
    val aliveWorkerNum = shuffledAliveWorkers.size
    var curPos = 0

    for (query <- waitingQueries.toList) { // iterate over immutable copy of waiting drivers

      var launched = false
      var visitedWorkerNum = 0
      while (visitedWorkerNum < aliveWorkerNum && !launched) {
        val worker = shuffledAliveWorkers(curPos)
        visitedWorkerNum += 1
        if (worker.memoryFree >= query.desc.mem && worker.coresFree >= query.desc.cores) {
          launchQuery(worker, query)
          waitingQueries -= query
          launched = true
        }
        curPos = (curPos + 1) % aliveWorkerNum
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
