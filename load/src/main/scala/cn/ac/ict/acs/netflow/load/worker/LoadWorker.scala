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
import akka.actor._
import akka.remote.{ DisassociatedEvent, RemotingLifecycleEvent }

import org.joda.time.DateTime
import cn.ac.ict.acs.netflow._
import cn.ac.ict.acs.netflow.load.{LoadConf, LoadMessages}
import cn.ac.ict.acs.netflow.load.master.LoadMaster
import cn.ac.ict.acs.netflow.util._

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.concurrent.duration._

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

  var coresUsed = 0
  var memoryUsed = 0
  var connectionAttemptCount = 0

  var registrationRetryTimer: Option[Cancellable] = None

  // load data
  val defaultWriterNum = conf.getInt(LoadConf.WRITER_NUMBER, 1)
  val maxQueueNum = conf.getInt(LoadConf.QUEUE_MAXPACKAGE_NUM, 100000000)
  val warnThreshold = {
    val threshold = conf.getInt(LoadConf.QUEUE_WARN_THRESHOLD, 70)
    if (0 < threshold && threshold < 100) threshold else 70
  }

  // buffer list
  val netflowBuff =
    new WrapBufferQueue(maxQueueNum, warnThreshold,
      DefaultLoadBalanceStrategy.loadBalanceWorker,
      () => master! BufferOverFlow)

  // load Service
  val loadserver = new LoaderService(netflowBuff, conf)

  // receiver Service
  val receiverserver = new Receiver(netflowBuff, conf)

  /** whole load thread's current combine file timestamp **/
  val combineTimeStamp = new ArrayBuffer[Long]()

  val workerIP = InetAddress.getLocalHost.getHostAddress

  def coresFree: Int = cores - coresUsed
  def memoryFree: Int = memory - memoryUsed

  override def preStart(): Unit = {
    assert(!registered)
    logInfo("[Netflow] Starting NetFlow loadWorker %s:%d with %d cores, %s RAM".format(
      host, port, cores, Utils.megabytesToString(memory)))
    logInfo(s"[Netflow] Running NetFlow version ${cn.ac.ict.acs.netflow.NETFLOW_VERSION}")
    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])

    logInfo(s"[Netflow] Init write parquet pool, and will start $defaultWriterNum threads")
    loadserver.initParquetWriterPool(defaultWriterNum)
    receiverserver.start()

    registerWithMaster()
  }

  override def postStop(): Unit = {
    registrationRetryTimer.foreach(_.cancel())

    logInfo(s"Close receiver server and load server!")
    receiverserver.interrupt()
    loadserver.stopAllWriterThreads()
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
      if (connected)  master ! Heartbeat(workerId)

    case x: DisassociatedEvent if x.remoteAddress == masterAddress =>
      logInfo(s"[Netflow] $x Disassociated !")
      masterDisconnected()

    // the message is sent by loadService to tell load worker to combine whole parquet
    case CloseParquet(timeStamp) =>
      combineTimeStamp += timeStamp
      if (combineTimeStamp.size == loadserver.curThreadsNum) {
        combineTimeStamp.sortWith(_ < _)
        if (combineTimeStamp.head == combineTimeStamp.last) {
          if (connected) master ! CloseParquet(combineTimeStamp.head)
        } else {
          logWarning(s"Current total thread is ${loadserver.curThreadsNum}}, " +
            s"and all thread's stamp time in a dictionary should be same, " +
            s"but now ${combineTimeStamp.mkString("<=")}. ")

          if (connected) master ! CloseParquet(combineTimeStamp.head)
        }
        combineTimeStamp.clear()
      }

    // this message is sent by master to assigned this worker to run combine service
    case CombineParquet(fileStamp) =>
      new CombineService(fileStamp, master, conf).start()

    // master message (send by master to get buffer information)
    case BufferInfo =>
      if (connected)
        master! BufferSimpleReport(workerIP, netflowBuff.currUsageRate())

    // scheduler message (only send by itself)
    case BuffHeatBeat =>
      if (connected)
        master! BufferWholeReport(workerIP, netflowBuff.currUsageRate(), netflowBuff.maxQueueNum, netflowBuff.size)

    case AdjustThread =>
      loadserver.adjustWritersNum(loadserver.curThreadsNum + 1)

    case updateBGP(bgpIds, bgpDatas) =>

  }

  private def changeMaster(url: String, webUrl: String): Unit = {
    activeMasterUrl = url
    activeMasterWebUiUrl = webUrl

    master = context.actorSelection(
      LoadMaster.toAkkaUrl(activeMasterUrl, AkkaUtils.protocol(context.system)))
    masterAddress = LoadMaster.toAkkaAddress(activeMasterUrl, AkkaUtils.protocol(context.system))
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
      actor ! RegisterWorker(workerId, host, port, cores, memory, webUiPort, workerIP, receiverserver.port)
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
          master ! RegisterWorker(workerId, host, port, cores, memory, webUiPort, workerIP, receiverserver.port)
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


  // implement the load balance strategy
  private object DefaultLoadBalanceStrategy extends LoadBalanceStrategy with Logging {

    private var lastRate = 0.0
    private var lastThreadNum = 0

    override def loadBalanceWorker(): Unit = {
      logInfo("load Balance Worker")
      val currentThreadNum = loadserver.curThreadsNum
      val currentThreadsAverageRate = calculateAverageRate(loadserver.curPoolRate)

      if (lastThreadNum == 0) {
        // the first time to exec load balance, so we only increase thread number
        loadserver.adjustWritersNum(currentThreadNum + 1)

        lastThreadNum = currentThreadNum
        lastRate = currentThreadsAverageRate
      } else {
        if (currentThreadsAverageRate > lastRate) {
          loadserver.adjustWritersNum(currentThreadNum + 1)
        } else {
          if (master != null) {
            master! BuffersWarn(host)
          } else {
            loadserver.adjustWritersNum(currentThreadNum + 1)
          }
        }
      }
    }

    private def calculateAverageRate(ratesList: util.ArrayList[Double]): Double = {
      var sum = 1.0
      var idx = 0
      while(idx < ratesList.size()){
        sum += ratesList.get(idx)
        idx += 1
      }
      sum / ratesList.size()
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
      LoadMaster.toAkkaUrl(_, AkkaUtils.protocol(actorSystem)))
    actorSystem.actorOf(Props(classOf[LoadWorker], host, boundPort, webUiPort, cores,
      memory, masterAkkaUrls, sysName, actorName, conf), name = actorName)
    (actorSystem, boundPort)
  }
}
