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
package cn.ac.ict.acs.netflow.load.worker

import java.net.InetAddress
import java.util
import java.util.UUID

import scala.util.Random
import scala.concurrent.duration._

import akka.actor._
import akka.remote.{ DisassociatedEvent, RemotingLifecycleEvent }

import org.joda.time.DateTime
import cn.ac.ict.acs.netflow._
import cn.ac.ict.acs.netflow.load.{ LoadConf, LoadMessages }
import cn.ac.ict.acs.netflow.util._
import cn.ac.ict.acs.netflow.metrics.MetricsSystem

class LoadWorker(
  host: String,
  port: Int,
  webUiPort: Int,
  cores: Int,
  memory: Int,
  masterAkkaUrls: Array[String],
  actorSystemName: String,
  actorName: String,
  val conf: NetFlowConf)
    extends Actor with ActorLogReceive with Logging {

  import DeployMessages._
  import LoadMessages._
  import context.dispatcher

  Utils.checkHost(host, "Expected hostname")
  assert(port > 0)

  val metricsSystem = MetricsSystem.createMetricsSystem("worker", conf)
  val workerSource = new LoadWorkerSource(this)

  val createTimeFormat = TimeUtils.createFormat

  // Send a heartbeat every (heartbeat timeout) / 4 milliseconds
  val HEARTBEAT_MILLIS = conf.getLong("netflow.LoadWorker.timeout", 60) * 1000 / 4

  // Model retries to connect to the master, after Hadoop's model.
  // The first six attempts to reconnect are in shorter intervals (between 5 and 15 seconds)
  // Afterwards, the next 10 attempts are between 30 and 90 seconds.
  // A bit of randomness is introduced so that not all of the workers attempt to reconnect at
  // the same time.
  val INITIAL_REGISTRATION_RETRIES = 6 // initial registration retries
  val TOTAL_REGISTRATION_RETRIES = INITIAL_REGISTRATION_RETRIES + 10 // total registration retries

  val FUZZ_MULTIPLIER_INTERVAL_LOWER_BOUND = 0.500 // fuzz multiplier interval lower bound
  val REGISTRATION_RETRY_FUZZ_MULTIPLIER = { // registration retry fuzz multiplier
    val randomNumberGenerator = new Random(UUID.randomUUID.getMostSignificantBits)
    randomNumberGenerator.nextDouble + FUZZ_MULTIPLIER_INTERVAL_LOWER_BOUND
  }

  val INITIAL_REGISTRATION_RETRY_INTERVAL = // initial registration retry interval
    math.round(10 * REGISTRATION_RETRY_FUZZ_MULTIPLIER).seconds
  val PROLONGED_REGISTRATION_RETRY_INTERVAL = // prolonged registration retry interval
    math.round(60 * REGISTRATION_RETRY_FUZZ_MULTIPLIER).seconds

  var master: ActorSelection = null
  var masterAddress: Address = null
  var activeMasterUrl: String = ""
  var activeMasterWebUiUrl: String = ""

  val akkaUrl = AkkaUtils.address(
    AkkaUtils.protocol(context.system),
    actorSystemName,
    host,
    port,
    actorName)

  @volatile var registered = false
  @volatile var connected = false

  val workerId = generateWorkerId()
  val workerIP = InetAddress.getLocalHost.getHostAddress
  var coresUsed = 0
  var memoryUsed = 0
  var connectionAttemptCount = 0
  var registrationRetryTimer: Option[Cancellable] = None

  
  // buffer list
  val netflowBuff = new WrapBufferQueue(
    DefaultLoadBalanceStrategy2.loadBalanceWorker,
    () => master ! BuffersWarn(workerIP), conf)

  // load Service, selfActor for tell worker to combine the directory witch has finish writing.
  val loadServer = new LoaderService(self, netflowBuff, conf)

  // receiver Service
  val receiverServer = new Receiver(netflowBuff, conf)

  // whole load thread's current combine file timestamp
  private var combineTimeStamp: Long = _

  def coresFree: Int = cores - coresUsed
  def memoryFree: Int = memory - memoryUsed

  override def preStart(): Unit = {
    assert(!registered)
    logInfo("[Netflow] Starting NetFlow loadWorker %s:%d with %d cores, %s RAM".format(
      host, port, cores, Utils.megabytesToString(memory)))
    logInfo(s"[Netflow] Running NetFlow version ${cn.ac.ict.acs.netflow.NETFLOW_VERSION}")
    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])

    val defaultWriterNum = conf.getInt(LoadConf.WRITER_NUMBER, 1)
    
    logInfo(s"[Netflow] Init write parquet pool, and will start $defaultWriterNum threads")
    loadServer.initParquetWriterPool(defaultWriterNum)
    receiverServer.start()

    registerWithMaster()
    metricsSystem.registerSource(workerSource)
    metricsSystem.start()
  }

  override def postStop(): Unit = {
    metricsSystem.report()
    registrationRetryTimer.foreach(_.cancel())

    logInfo(s"Close receiver server and load server!")
    receiverServer.interrupt()
    loadServer.stopAllWriterThreads()
    metricsSystem.stop()
  }

  override def receiveWithLogging = {
    case RegisteredWorker(masterUrl, masterWebUrl) =>
      logInfo("Successfully registered with load Master " + masterUrl)
      registered = true
      changeMaster(masterUrl, masterWebUrl)
      context.system.scheduler.schedule(0.millis, HEARTBEAT_MILLIS.millis, self, SendHeartbeat)
      context.system.scheduler.schedule(0.millis, HEARTBEAT_MILLIS.millis, self, BuffHeatBeat)

    case RegisterWorkerFailed(message) =>
      if (!registered) {
        logError("[Netflow] Worker registration failed: " + message)
        System.exit(1)
      }

    case ReconnectWorker(masterUrl) =>
      logInfo(s"[Netflow] Master with url $masterUrl requested this worker to reconnect.")
      registerWithMaster()

    case ReregisterWithMaster =>
      reregisterWithMaster()

    case MasterChanged(masterUrl, masterWebUrl) =>
      // Get this message because of master recovery
      logInfo("[Netflow] Master has changed, new master is at " + masterUrl)
      changeMaster(masterUrl, masterWebUrl)

    case SendHeartbeat =>
      if (connected) master ! Heartbeat(workerId)

    case x: DisassociatedEvent if x.remoteAddress == masterAddress =>
      logInfo(s"[Netflow] $x Disassociated !")
      masterDisconnected()

    /**
     * deal with combine message
     */
    // the message is sent by loadService to tell load worker to combine whole parquet
    case CloseParquet(timeStamp) =>
      if (connected && combineTimeStamp != timeStamp) {
        combineTimeStamp = timeStamp
        master ! CloseParquet(combineTimeStamp)
      }

    // this message is sent by master to assigned this worker to run combine service
    case CombineParquet(fileStamp) =>
      new CombineService(fileStamp, master, conf).start()

    /**
     * deal with buffer info message
     */
    // master message (send by master to get buffer information)
    case BufferInfo =>
      if (connected) {
        master ! BufferSimpleReport(workerIP, netflowBuff.currUsageRate())
      }

    // scheduler message (only send by itself)
    case BuffHeatBeat =>
      if (connected) {
        master ! BufferWholeReport(workerIP, netflowBuff.currUsageRate(),
          netflowBuff.maxQueueNum, netflowBuff.currSize)
      }

    /**
     * deal with balance
     */
    case AdjustThread =>
      if (loadServer.curThreadsNum < cores) {
        loadServer.adjustWritersNum(loadServer.curThreadsNum + 1)
      }

    /**
     * deal with BGP
     */
    case updateBGP(bgpIds, bgpDatas) =>
      updateBGP(bgpIds, bgpDatas)
  }

  private def changeMaster(url: String, webUrl: String): Unit = {
    activeMasterUrl = url
    activeMasterWebUiUrl = webUrl

    master = context.actorSelection(
      AkkaUtils.toLMAkkaUrl(activeMasterUrl, AkkaUtils.protocol(context.system)))
    masterAddress = AkkaUtils.toLMAkkaAddress(activeMasterUrl, AkkaUtils.protocol(context.system))
    connected = true

    // Cancel any outstanding re-registration attempts because we found a new master
    registrationRetryTimer.foreach(_.cancel())
    registrationRetryTimer = None
  }

  private def registerWithMaster() {
    // DisassociatedEvent may be triggered multiple times, so don't attempt registration
    // if there are outstanding registration attempts scheduled.
    registrationRetryTimer match {
      case None =>
        registered = false
        tryRegisterAllMasters()
        connectionAttemptCount = 0
        registrationRetryTimer = Some {
          context.system.scheduler.schedule(INITIAL_REGISTRATION_RETRY_INTERVAL,
            INITIAL_REGISTRATION_RETRY_INTERVAL, self, ReregisterWithMaster)
        }
      case Some(_) =>
        logInfo("[Netflow] Not spawning another attempt to register with the master," +
          " since there is an attempt scheduled already.")
    }
  }

  private def tryRegisterAllMasters() {
    for (masterAkkaUrl <- masterAkkaUrls) {
      logInfo("[Netflow] Connecting to Load Master " + masterAkkaUrl + "...")
      val actor = context.actorSelection(masterAkkaUrl)
      actor ! RegisterWorker(workerId, host, port, cores, memory,
        webUiPort, workerIP, receiverServer.port)
    }
  }

  /**
   * Re-register with the master because a network failure or a master failure has occurred.
   * If the re-registration attempt threshold is exceeded, the worker exits with error.
   * Note that for thread-safety this should only be called from the actor.
   */
  private def reregisterWithMaster(): Unit = {
    Utils.tryOrExit {
      connectionAttemptCount += 1
      if (registered) {
        registrationRetryTimer.foreach(_.cancel())
        registrationRetryTimer = None
      } else if (connectionAttemptCount <= TOTAL_REGISTRATION_RETRIES) {
        logInfo(s"Retrying connection to master (attempt # $connectionAttemptCount)")
        /**
         * Re-register with the active master this worker has been communicating with. If there
         * is none, then it means this worker is still bootstrapping and hasn't established a
         * connection with a master yet, in which case we should re-register with all masters.
         *
         * It is important to re-register only with the active master during failures. Otherwise,
         * if the worker unconditionally attempts to re-register with all masters, the following
         * race condition may arise and cause a "duplicate worker" error detailed in SPARK-4592:
         *
         *   (1) Master A fails and Worker attempts to reconnect to all masters
         *   (2) Master B takes over and notifies Worker
         *   (3) Worker responds by registering with Master B
         *   (4) Meanwhile, Worker's previous reconnection attempt reaches Master B,
         *       causing the same Worker to register with Master B twice
         *
         * Instead, if we only register with the known active master, we can assume that the
         * old master must have died because another master has taken over. Note that this is
         * still not safe if the old master recovers within this interval, but this is a much
         * less likely scenario.
         */
        if (master != null) {
          master ! RegisterWorker(workerId, host, port, cores, memory, webUiPort,
            workerIP, receiverServer.port)
        } else {
          // We are retrying the initial registration
          tryRegisterAllMasters()
        }
        // We have exceeded the initial registration retry threshold
        // All retries from now on should use a higher interval
        if (connectionAttemptCount == INITIAL_REGISTRATION_RETRIES) {
          registrationRetryTimer.foreach(_.cancel())
          registrationRetryTimer = Some {
            context.system.scheduler.schedule(PROLONGED_REGISTRATION_RETRY_INTERVAL,
              PROLONGED_REGISTRATION_RETRY_INTERVAL, self, ReregisterWithMaster)
          }
        }
      } else {
        logError("All masters are unresponsive! Giving up.")
        System.exit(1)
      }
    }
  }

  private def masterDisconnected(): Unit = {
    logError("Connection to master failed! Waiting for master to reconnect...")
    connected = false
    registerWithMaster()
  }

  private def generateWorkerId(): String = {
    "load-worker-%s-%s-%d".format((new DateTime).toString(createTimeFormat), host, port)
  }

  private def updateBGP(key: Array[Int], value: Array[Array[Byte]]): Unit = {

  }

  // implement the load balance strategy
  private object DefaultLoadBalanceStrategy extends LoadBalanceStrategy with Logging {

    private var lastRate = 0.0
    private var lastThreadNum = 0
    private val maxLoadThreadsNum = cores / 2

    override def loadBalanceWorker(): Unit = {
      val currentThreadNum = loadServer.curThreadsNum
      val currentThreadsAverageRate = calculateAverageRate(loadServer.curPoolRate)

      logInfo(s"current threads num is $currentThreadNum, " +
        s"and current threads rate is  $currentThreadsAverageRate. ")

      if (currentThreadNum >= maxLoadThreadsNum) {
        if (master != null) master ! BuffersWarn(workerIP)
        return
      }

      if (lastThreadNum == 0) {
        // the first time to exec load balance, so we only increase thread number
        loadServer.adjustWritersNum(currentThreadNum + 1)
        logWarning("add a new thread")
        lastThreadNum = currentThreadNum
        lastRate = currentThreadsAverageRate
      } else {
        if (currentThreadsAverageRate > lastRate) {
          loadServer.adjustWritersNum(currentThreadNum + 1)
          logWarning("add a new thread")
        } else {
          if (master != null) {
            master ! BuffersWarn(host)
            logWarning("request with master")
          }
        }
      }
    }

    private def calculateAverageRate(ratesList: util.ArrayList[Double]): Double = {
      var sum = 1.0
      var idx = 0
      while (idx < ratesList.size()) {
        sum += ratesList.get(idx)
        idx += 1
      }
      sum / ratesList.size()
    }
  }

  private object DefaultLoadBalanceStrategy2 extends LoadBalanceStrategy with Logging{

    private val maxLoadThreadsNum = cores / 2

    override def loadBalanceWorker(): Unit = {

      val curThreadNum = loadServer.curThreadsNum
      if(curThreadNum < maxLoadThreadsNum){
        loadServer.adjustWritersNum(curThreadNum + 1)
        logInfo(s"Add new thread")
      }
    }
  }
}

object LoadWorker extends Logging {
  val systemName = "netflowLoadWorker"
  val actorName = "LoadWorker"

  def main(argStrings: Array[String]) {
    SignalLogger.register(log)
    val conf = new NetFlowConf
    val args = new LoadWorkerArguments(argStrings, conf)

    val (actorSystem, _) = startSystemAndActor(args.host, args.port, args.webUiPort,
      args.cores, args.memory, args.masters, conf = conf)
    actorSystem.awaitTermination()
  }

  def startSystemAndActor(
    host: String,
    port: Int,
    webUiPort: Int,
    cores: Int,
    memory: Int,
    masterUrls: Array[String],
    workerNumber: Option[Int] = None,
    conf: NetFlowConf = new NetFlowConf): (ActorSystem, Int) = {

    val sysName = systemName + workerNumber.map(_.toString).getOrElse("")
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(sysName, host, port, conf)
    val masterAkkaUrls = masterUrls.map(
      AkkaUtils.toLMAkkaUrl(_, AkkaUtils.protocol(actorSystem)))
    actorSystem.actorOf(Props(classOf[LoadWorker], host, boundPort, webUiPort, cores,
      memory, masterAkkaUrls, sysName, actorName, conf), name = actorName)
    (actorSystem, boundPort)
  }
}
