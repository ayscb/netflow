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
package cn.ac.ict.acs.netflow.query.master

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.Await

import akka.actor._
import akka.pattern.ask
import akka.remote.{DisassociatedEvent, RemotingLifecycleEvent}
import akka.serialization.SerializationExtension
import akka.util.Timeout

import org.joda.time.DateTime

import org.apache.spark.deploy.rest._

import cn.ac.ict.acs.netflow._
import cn.ac.ict.acs.netflow.util._
import cn.ac.ict.acs.netflow.query.RestMessages
import cn.ac.ict.acs.netflow.query.broker.ValidJobDescription
import cn.ac.ict.acs.netflow.ha.{MonarchyLeaderAgent, LeaderElectable, LeaderElectionAgent}

class QueryMaster(
    host: String,
    port: Int,
    webUiPort: Int,
    val conf: NetFlowConf)
  extends Actor with ActorLogReceive with LeaderElectable with Logging {

  import MasterMessages._
  import QueryMasterMessages._
  import RestMessages._
  import DeployMessages._
  import JobMessages._

  import context.dispatcher

  val createTimeFormat = TimeUtils.createFormat
  val showFormat = TimeUtils.showFormat
  val BROKER_TIMEOUT = conf.getLong("netflow.broker.timeout", 60) * 1000
  val RETAINED_QUERY = conf.getInt("netflow.deploy.retainedQuerys", 200)

  implicit val askTimeOut = Timeout(AkkaUtils.askTimeout(conf))

  // TODO find right place to set spark configurations used by netflow
  val sparkMasterUrl = conf.get("netflow.spark.master", "spark://Yijie-MBP.local:7077")
  val sparkRestMasterUrl = conf.get("netflow.spark.rest.master", "spark://Yijie-MBP.local:6066")

  val sparkHome = System.getenv("SPARK_HOME")
  require(sparkHome != null, "SPARK_HOME env must be set")

  // Remove a dead broker after given interval
  val REAPER_ITERATIONS = conf.getInt("netflow.dead.broker.persistence", 15)
  // Master recovery mode
  val RECOVERY_MODE = conf.get("netflow.deploy.recoveryMode", "NONE")

  val RESULT_CACHE_SIZE = conf.getInt("netflow.adhoc.result.cacheSize", 30)

  // This may contain dead workers
  val brokers = new mutable.HashSet[BrokerInfo]
  // Current alive workers
  val idToBroker = new mutable.HashMap[String, BrokerInfo]
  // Current alive workers
  val addressToBroker = new mutable.HashMap[Address, BrokerInfo]

  // Registered Job query may be scheduled and cancel later
  val idToJobScheduler = new mutable.HashMap[String, Cancellable]

  val idToJobs = new mutable.HashMap[String, JobInfo]
  val idToRunningJobs = new mutable.HashMap[String, JobInfo]

  Utils.checkHost(host, "Expected hostname")

  var nextJobNumber = 0

  val queryMasterUrl = "netflow-query://" + host + ":" + port
  var queryMasterWebUIUrl: String = _

  var state = QueryMasterRecoveryState.STANDBY

  var persistenceEngine: MasterPersistenceEngine = _

  var leaderElectionAgent: LeaderElectionAgent = _

  private var recoveryCompletionTask: Cancellable = _

  var sparkRestClient: RestSubmissionClient = _

  var resultTracker: ActorRef = _

  // preparing args and settings to launch spark jobs
  val defaultJar = s"file:$sparkHome/lib/sparkjob-$NETFLOW_VERSION.jar"
  val defaultMainClass = "cn.ac.ict.acs.netflow.JobActor"

  val defaultSparkProperties = new mutable.HashMap[String, String]
  defaultSparkProperties("spark.master") = sparkMasterUrl
  defaultSparkProperties("spark.app.name") = defaultMainClass
  defaultSparkProperties("spark.jars") = defaultJar
  defaultSparkProperties("spark.driver.supervise") = "true"
  val defaultEnvironmentVariables = new mutable.HashMap[String, String]
  defaultEnvironmentVariables("SPARK_SCALA_VERSION") = SCALA_BINARY_VERSION
  defaultEnvironmentVariables("SPARK_HOME") = sparkHome
  defaultEnvironmentVariables("SPARK_ENV_LOADED") = "1"

  override def preStart(): Unit = {
    logInfo("Starting NetFlow QueryMaster at " + queryMasterUrl)
    logInfo(s"Running NetFlow version ${cn.ac.ict.acs.netflow.NETFLOW_VERSION}")
    // Listen for remote client disconnection events, since they don't go through Akka's watch()
    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])

    // TODO: a pseudo webuiurl here
    queryMasterWebUIUrl = "http://" + host + ":" + webUiPort

    context.system.scheduler.schedule(0.millis, BROKER_TIMEOUT.millis, self, CheckForBrokerTimeOut)

    val (persistenceEngine_, leaderElectionAgent_) = RECOVERY_MODE match {
      case "ZOOKEEPER" =>
        logInfo("Persisting recovery state to ZooKeeper")
        val zkFactory =
          new ZKRecoveryModeFactory(conf, SerializationExtension(context.system))
        (zkFactory.createPersistenceEngine(), zkFactory.createLeaderElectionAgent(this))
      case _ =>
        logInfo("No state persisted as a MonarchyLeader")
        (new QueryMasterBHPersistenceEngine(), new MonarchyLeaderAgent(this))
    }
    persistenceEngine = persistenceEngine_
    leaderElectionAgent = leaderElectionAgent_

    sparkRestClient = new RestSubmissionClient(sparkRestMasterUrl)
    resultTracker = context.actorOf(
      Props(classOf[JobResultTracker], RESULT_CACHE_SIZE), "JobResultTracker")
  }

  override def preRestart(reason: Throwable, message: Option[Any]) {
    super.preRestart(reason, message) // calls postStop()!
    logError("QueryMaster actor restarted due to exception", reason)
  }

  override def postStop(): Unit = {
    persistenceEngine.close()
    leaderElectionAgent.stop()
    idToJobScheduler.values.foreach(_.cancel())
  }

  override def appointLeader() = {
    self ! AppointedAsLeader
  }

  override def revokeLeadership() = {
    self ! RevokedLeadership
  }

  def beginRecovery(
    storedJobs: Seq[JobInfo],
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
          logInfo(s"broker has been re-registered: $brokerId")
          broker.state = BrokerState.ALIVE

        case None =>
          logWarning(s"Scheduler state from unknown broker: $brokerId")
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

    case JobLaunched(jobId) => {
      if (state != QueryMasterRecoveryState.ALIVE) {
        val msg = s"Can only accept job registration in ALIVE state. Current state: $state."
        sender ! WrongMaster(msg)
      } else {
        logInfo(s"Job $jobId get scheduled on Spark.")
        idToJobs.get(jobId) match {
          case Some(j) => {
            j._startTime = Some(DateTime.now()) // a place to call start
            val tp = if (j.jobType == JobType.ONLINE) "STREAM" else "SQL"
            val rt = if (j.jobType == JobType.ADHOC) Some(resultTracker) else None
            sender ! JobInitialize(tp, j.query, j.outputPath, rt, sparkMasterUrl)
          }
          case None =>
            sender ! JobNotFound
        }
      }
    }

    case JobFinished(jobId) => {
      if (state != QueryMasterRecoveryState.ALIVE) {
        val msg = s"Can only accept job registration in ALIVE state. Current state: $state."
        sender ! WrongMaster(msg)
      } else {
        logInfo(s"Job $jobId finished normally.")
        idToJobs.get(jobId) match {
          case Some(j) => {
            j.finish
            sender ! JobEndACK
          }
          case None =>
            sender ! JobNotFound
        }
      }
    }

    case JobFailed(jobId, err) => {
      if (state != QueryMasterRecoveryState.ALIVE) {
        val msg = s"Can only accept job registration in ALIVE state. Current state: $state."
        sender ! WrongMaster(msg)
      } else {
        logInfo(s"Job $jobId failed with exception: ${err.toString}.")
        logInfo(s"${err.getStackTraceString}")
        idToJobs.get(jobId) match {
          case Some(j) => {
            j.fail(Some(err.toString))
            sender ! JobEndACK
          }
          case None =>
            sender ! JobNotFound
        }
      }
    }

    case CheckForBrokerTimeOut => {
      timeOutDeadBrokers()
    }

    case RestQueryMasterStatusRequest => {
      sender ! RestQueryMasterStatusResponse(NETFLOW_VERSION, "v1",
        idToBroker.keys.toSeq, idToJobs.keys.toSeq)
    }

    case RestAllJobsInfoRequest => {
      val list = new mutable.ArrayBuffer[JobInfoAbstraction]()
      idToJobs.foreach { case (id, j) =>
        list += JobInfoAbstraction(id, j.submitTime.toString(createTimeFormat),
          j.query.sql.take(15) + " ...", j._state.toString, j._submissionId,
          j._startTime.map(_.toString(createTimeFormat)),
          j._endTime.map(_.toString(createTimeFormat)),
          j._message, j._driverState)
      }
      sender ! RestAllJobsInfoResponse(list.toSeq)

    }

    case RestSubmitJobRequest(jobDesc) => {
      if (state != QueryMasterRecoveryState.ALIVE) {
        val msg = s"Can only accept job registration in ALIVE state. Current state: $state."
        sender ! RestFailureResponse(msg)
      } else {
        logInfo(s"New Job registered : [type: ${jobDesc.jobType}, query: ${jobDesc.query.sql}]")
        val jobInfo = createJob(jobDesc)
        idToJobs(jobInfo.id) = jobInfo
        persistenceEngine.addJob(jobInfo)
        registerInScheduler(jobInfo)
        sender ! RestSubmitJobResponse(
          jobInfo.id, true, Some(s"Job successfully submitted as ${jobInfo.id}"))
      }
    }

    case RestJobInfoRequest(jobId) => {
      if (state != QueryMasterRecoveryState.ALIVE) {
        val msg = s"Can only accept job registration in ALIVE state. Current state: $state."
        sender ! RestFailureResponse(msg)
      } else {
        logInfo(s"Request detailed information of job: $jobId")
        idToJobs.get(jobId) match {
          case Some(j) => {
            if (j._submissionId.isDefined) {
              requestJobStatusOnSpark(j)
            }
            sender ! j.toRestJobInfoResponse
          }

          case None =>
            sender ! RestFailureResponse(s"Job $jobId not exist.")
        }
      }
    }

    case RestJobStatusRequest(jobId) => {
      if (state != QueryMasterRecoveryState.ALIVE) {
        val msg = s"Can only accept job registration in ALIVE state. Current state: $state."
        sender ! RestFailureResponse(msg)
      } else {
        logInfo(s"Request status of Job: $jobId")
        idToJobs.get(jobId) match {
          case Some(j) => {
            // Update job status if it's running on Spark
            if (j._submissionId.isDefined) {
              requestJobStatusOnSpark(j)
            }

            // Try to fetch result from ResultTracker
            if (j._state == JobState.FINISHED && j.jobType == JobType.ADHOC) {
              val receiver = sender
              for (result <- (resultTracker ? GetJobResult(jobId))){
                val cachedValue = result match {
                  case JobResult(_, v) => Some(v)
                  case JobNotFound => None
                }
                receiver ! RestJobStatusResponse(jobId, j._state.toString,
                  j.submitTime.toString(showFormat), j._startTime.map(_.toString(showFormat)),
                  j._endTime.map(_.toString(showFormat)), cachedValue, j._message)
              }

            } else {
             sender !
               RestJobStatusResponse(jobId, j._state.toString,
                 j.submitTime.toString(showFormat), j._startTime.map(_.toString(showFormat)),
                 j._endTime.map(_.toString(showFormat)), None, j._message)
            }
          }
          case None =>
            sender ! RestFailureResponse(s"Job $jobId not exist.")
        }
      }
    }

    case RestKillJobRequest(jobId) => {
      if (state != QueryMasterRecoveryState.ALIVE) {
        val msg = s"Can only cancel job in ALIVE state. Current state: $state."
        sender ! RestFailureResponse(msg)
      } else {
        logInfo(s"Asked to kill job ${jobId}")
        persistenceEngine.removeJob(jobId)
        idToJobScheduler.get(jobId).map(_.cancel())
        idToJobs.get(jobId) match {
          case Some(j) =>
            if (j._state == JobState.RUNNABLE || j._state == JobState.SCHEDULED) {
              j.kill
              sender ! RestKillJobResponse(jobId, true, Some("Job kill while waiting"))
            } else if (j._state == JobState.RUNNING) {
              if (j._submissionId.isDefined) {
                val success = killJobInSpark(j)
                sender ! RestKillJobResponse(jobId, success, None)
              } else {// this should never happen
                logError("Running job without submissionId")
                assert(false)
              }
            } else {
              sender ! RestKillJobResponse(jobId, false, Some(s"Job of state: ${j._state}"))
            }
          case None =>
            sender ! RestKillJobResponse(jobId, false,
              Some(s"Asked to kill an unknown job $jobId"))
        }
      }
    }

    case ScheduleJob(jobId) => {
      logInfo(s"Schedule job $jobId")
      idToJobs.get(jobId) match {
        case Some(j) => {
          submitToSpark(j)
        }
        case None =>{
          val msg = s"Job $jobId does not exist"
          logWarning(msg)
        }
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

  def createJob(desc: ValidJobDescription): JobInfo = {
    def newJobId(submitDate: DateTime, tpe: JobType.Value): String = {
      val jobId = "job-%s-%s-%06d".format(
        tpe.toString.toLowerCase,
        submitDate.toString(createTimeFormat),
        nextJobNumber)
      nextJobNumber += 1
      jobId
    }

    val now = DateTime.now()

    val id = newJobId(now, desc.jobType)

    val jar = desc.jar.getOrElse(defaultJar)
    val mainClass = desc.mainClass.getOrElse(defaultMainClass)

    // JobActor's main method args
    val defaultAppArgs = new mutable.ArrayBuffer[String]()
    defaultAppArgs += queryMasterUrl
    defaultAppArgs += id

    val appArgs = desc.appArgs.getOrElse(defaultAppArgs.toArray)

    val effectiveSparkProperties =
      desc.sparkProperties.map(_ ++ defaultSparkProperties).
        getOrElse(defaultSparkProperties)

    val effectiveEnvironmentVariables =
    desc.environmentVariables.map(_ ++ defaultEnvironmentVariables).
      getOrElse(defaultEnvironmentVariables)

    val outPath = desc.outputPath.getOrElse("/tmp")

    new JobInfo(id, desc.description, desc.jobType,
      now, desc.deferTime, desc.frequency,
      desc.query, jar, mainClass, appArgs,
      effectiveSparkProperties.toMap[String, String],
      effectiveEnvironmentVariables.toMap[String, String],
      outPath)

  }

  def delayRound(initialDelay: Long, interval: Long, now: Long): Long = {
    if (now < initialDelay) {
      initialDelay - now
    } else {
      now - initialDelay - ((now - initialDelay) / interval) * interval
    }
  }

  def registerInScheduler(job: JobInfo): Unit = {
    val now = System.currentTimeMillis()
    val intendStart = job.submitTime.getMillis + job.deferTime.toMillis
    val timerTask = job.jobType match {
      case JobType.ADHOC =>
        // if time passed, we do not schedule it again
        if (now - intendStart <= 30 * 1000) {
          val delay = intendStart - now
          context.system.scheduler.scheduleOnce(
            delay.millis, self, ScheduleJob(job.id))
        } else {
          logError(s"Job ${job.id} was late for scheduling")
          null
        }

      case JobType.ONLINE =>
        // schedule the job directly if passed intendStart
        val delay = math.max(0, intendStart - now)
        context.system.scheduler.scheduleOnce(delay.millis, self, ScheduleJob(job.id))

      case JobType.REPORT =>
        val delay = delayRound(intendStart, job.frequency.get.toMillis, now)
        context.system.scheduler.schedule(
          delay.millis, job.frequency.get,
          self, ScheduleJob(job.id))
    }

    if (timerTask != null) {
      idToJobScheduler(job.id) = timerTask
    }
  }

  def submitToSpark(job: JobInfo) = {
    try {
      val request = sparkRestClient.constructSubmitRequest(job.jar, job.mainClass,
        job.appArgs, job.sparkProperties, job.environmentVariables)
      val response: SubmitRestProtocolResponse = sparkRestClient.createSubmission(request)
      response match {
        case s: CreateSubmissionResponse =>
          if (s.success) {
            job.start(s.submissionId)
            idToRunningJobs(job.id) = job
          } else {
            job.fail(Some("Failed to submit"))
          }
        case unexpected =>
          job.fail(Some("Unexpected response from spark rest server"))
      }
    } catch {
      case e: Throwable =>
        logError(e.getMessage)
        job.fail(Some(e.getMessage))
    }
  }

  def requestJobStatusOnSpark(job: JobInfo) = {
    try {
      if (!job._submissionId.isDefined) {
        logError("We shouldn't request a job that haven't submitted to Spark")
      }
      assert(job._submissionId.isDefined)

      val response = sparkRestClient.requestSubmissionStatus(job._submissionId.get)
      response match {
        case s: SubmissionStatusResponse if s.success => {
          job._driverState = Some(s.driverState)
          s.driverState match {
            case "SUBMITTED" | "RUNNING" | "RELAUNCHING" =>
              job._state = JobState.RUNNING
            case "FINISHED" =>
              idToRunningJobs -= job.id
              job._state = JobState.FINISHED
            case "FAILED" | "ERROR" =>
              idToRunningJobs -= job.id
              job._state = JobState.FAILED
            case "UNKNOWN" =>
              job._state = JobState.UNKNOWN
            case "KILLED" =>
              idToRunningJobs -= job.id
              job._state = JobState.KILLED
          }
        }
        case unexpected =>
          job._state = JobState.UNKNOWN
          logWarning("Job state unknown")
      }
    } catch {
      case e: Throwable =>
        logError(e.getMessage)
        job.fail(Some(e.getMessage))
    }
  }

  def killJobInSpark(job: JobInfo): Boolean = {
    try {
      if (!job._submissionId.isDefined) {
        logError("We shouldn't request a job that haven't submitted to Spark")
      }
      assert(job._submissionId.isDefined)

      val response = sparkRestClient.killSubmission(job._submissionId.get)
      job.kill

      response.success
    } catch {
      case e: Throwable =>
        logError(e.getMessage)
        job.fail(Some(e.getMessage))
        false
    }
  }

}

object QueryMaster extends Logging {
  import MasterMessages._

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
    val (actorSystem, boundPort) =
      AkkaUtils.createActorSystem(QUERYMASTER_ACTORSYSTEM, host, port, conf)
    val actor = actorSystem.actorOf(
      Props(classOf[QueryMaster], host, boundPort, webUiPort, conf), QUERYMASTER_ACTOR)
    val timeout = AkkaUtils.askTimeout(conf)
    val portsRequest = actor.ask(BoundPortsRequest)(timeout)
    val portsResponse = Await.result(portsRequest, timeout).asInstanceOf[BoundPortsResponse]
    (actorSystem, boundPort, portsResponse.webUIPort)
  }
}
