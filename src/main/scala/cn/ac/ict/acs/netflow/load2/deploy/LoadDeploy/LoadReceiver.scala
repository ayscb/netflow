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

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.DatagramChannel
import java.util.UUID

import akka.actor._
import akka.remote.{DisassociatedEvent, RemotingLifecycleEvent}
import cn.ac.ict.acs.netflow.deploy.DeployMessages._
import cn.ac.ict.acs.netflow.deploy.Messages.SendHeartbeat
import cn.ac.ict.acs.netflow.load2.deploy.LoadMasterMessage.updateDestWorker
import cn.ac.ict.acs.netflow.{Logging, NetFlowConf}
import cn.ac.ict.acs.netflow.util.{SignalLogger, AkkaUtils, Utils, ActorLogReceive}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.util.Random
import scala.concurrent.duration._

/**
 * Created by ayscb on 2015/5/4.
 */
class LoadReceiver (
                    host: String,
                    port: Int,
                    webUiPort: Int,
                    masterAkkaUrls: Array[String],
                    actorSystemName: String,
                    actorName: String,
                    val conf: NetFlowConf)
    extends Actor with ActorLogReceive with Logging {

    import context.dispatcher

    Utils.checkHost(host, "Expected hostname")
    assert (port > 0)

    val createTimeFormat = DateTimeFormat.forPattern("yyyyMMddHHmmss")

    // Send a heartbeat every (heartbeat timeout) / 4 milliseconds
    val HEARTBEAT_MILLIS = conf.getLong("netflow.LoadReceiver.timeout", 60) * 1000 / 4

    // Model retries to connect to the master, after Hadoop's model.
    // The first six attempts to reconnect are in shorter intervals (between 5 and 15 seconds)
    // Afterwards, the next 10 attempts are between 30 and 90 seconds.
    // A bit of randomness is introduced so that not all of the workers attempt to reconnect at
    // the same time.
    val INITIAL_REGISTRATION_RETRIES = 6
    val TOTAL_REGISTRATION_RETRIES = INITIAL_REGISTRATION_RETRIES + 10
    val FUZZ_MULTIPLIER_INTERVAL_LOWER_BOUND = 0.500
    val REGISTRATION_RETRY_FUZZ_MULTIPLIER = {
      val randomNumberGenerator = new Random(UUID.randomUUID.getMostSignificantBits)
      randomNumberGenerator.nextDouble + FUZZ_MULTIPLIER_INTERVAL_LOWER_BOUND
    }
    val INITIAL_REGISTRATION_RETRY_INTERVAL = math.round(10 *
      REGISTRATION_RETRY_FUZZ_MULTIPLIER).seconds
    val PROLONGED_REGISTRATION_RETRY_INTERVAL = math.round(60
      * REGISTRATION_RETRY_FUZZ_MULTIPLIER).seconds

    // the load work to send the dataflow to
    var effectiveWorkerUrl : String = _
    var effectiveWorkerProt : Int = _

    var master: ActorSelection = null
    var masterAddress: Address = null
    var activeMasterUrl: String = ""
    var activeMasterWebUiUrl : String = ""
    val akkaUrl = AkkaUtils.address(
      AkkaUtils.protocol(context.system),
      actorSystemName,
      host,
      port,
      actorName)
    @volatile var registered = false
    @volatile var connected = false
    val workerId = generateWorkerId()

    var connectionAttemptCount = 1
    var registrationRetryTimer: Option[Cancellable] = None

    override def preStart(): Unit = {
      assert(!registered)
      logInfo("Starting NetFlow LoadReceiver %s:%d".format(host, port))
      logInfo(s"Running NetFlow version ${cn.ac.ict.acs.netflow.NETFLOW_VERSION}")

      context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])

      registerWithMaster()
    }

    override def postStop(): Unit = {
      registrationRetryTimer.foreach(_.cancel())
    }

    def receiveWithLogging = {
      case RegisteredWorker(masterUrl, masterWebUrl) =>
        logInfo("Successfully registered with load Master " + masterUrl)
        registered = true
        changeMaster(masterUrl, masterWebUrl)
        context.system.scheduler.schedule(0.millis, HEARTBEAT_MILLIS.millis, self, SendHeartbeat)

      case RegisterWorkerFailed(message) =>
        if (!registered) {
          logError("Worker registration failed: " + message)
          System.exit(1)
        }

      case ReconnectWorker(masterUrl) =>
        logInfo(s"Master with url $masterUrl requested this worker to reconnect.")
        registerWithMaster()

      case ReregisterWithMaster =>
        reregisterWithMaster()

      case MasterChanged(masterUrl, masterWebUrl) =>
        // Get this message because of master recovery
        logInfo("Master has changed, new master is at " + masterUrl)
        changeMaster(masterUrl, masterWebUrl)

      case SendHeartbeat =>
        if (connected) { master ! Heartbeat(workerId) }

      case x: DisassociatedEvent if x.remoteAddress == masterAddress =>
        logInfo(s"$x Disassociated !")
        masterDisconnected()

      case updateDestWorker( newhost, newport ) =>
        logInfo(s"the effect worker :$newhost:$newport")
        effectiveWorkerUrl = newhost
        effectiveWorkerProt = newport
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
          logInfo("Not spawning another attempt to register with the master, since there is an" +
            " attempt scheduled already.")
      }
    }

    private def tryRegisterAllMasters() {
      for (masterAkkaUrl <- masterAkkaUrls) {
        logInfo("Connecting to QueryMaster " + masterAkkaUrl + "...")
        val actor = context.actorSelection(masterAkkaUrl)
        actor ! RegisterWorker(workerId, host, port,0,0, webUiPort)
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
            master ! RegisterWorker(
              workerId, host, port, 0, 0, webUiPort)
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
      "worker-%s-%s-%d".format((new DateTime).toString(createTimeFormat), host, port)
    }

    private val udpChannel = DatagramChannel.open()
    // the function for transfer the data
    def sendData( data :Array[Byte] ) : Int = {
      udpChannel.send(ByteBuffer.wrap(data),new InetSocketAddress(host,port))
    }
  }

  object LoadReceiver extends Logging {
    val systemName = "netflowLoadReceiver"
    val actorName = "LoadReceiver"

    def main(argStrings: Array[String]) {
      SignalLogger.register(log)
      val conf = new NetFlowConf
      val args = new LoadReceiverArguments(argStrings, conf)
      val (actorSystem, _) = startSystemAndActor(args.host, args.port, args.webUiPort, args.masters, conf = conf)
      actorSystem.awaitTermination()
    }

    def startSystemAndActor(
                             host: String,
                             port: Int,
                             webUiPort: Int,
                             masterUrls: Array[String],
                             workerNumber: Option[Int] = None,
                             conf: NetFlowConf = new NetFlowConf): (ActorSystem, Int) = {

      val sysName = systemName + workerNumber.map(_.toString).getOrElse("")
      val (actorSystem, boundPort) = AkkaUtils.createActorSystem(sysName, host, port, conf)
      val masterAkkaUrls = masterUrls.map(
        LoadMaster.toAkkaUrl(_, AkkaUtils.protocol(actorSystem)))
      actorSystem.actorOf(Props(classOf[LoadReceiver], host, boundPort, webUiPort,
        masterAkkaUrls, sysName, actorName, conf), name = actorName)
      (actorSystem, boundPort)
    }
  }
