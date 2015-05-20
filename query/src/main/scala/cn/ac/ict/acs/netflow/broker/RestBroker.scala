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
package cn.ac.ict.acs.netflow.broker

import java.util.UUID

import scala.util.Random
import scala.concurrent.duration._

import akka.actor._
import akka.pattern.ask
import akka.io.IO
import akka.remote.{DisassociatedEvent, RemotingLifecycleEvent}

import org.json4s.DefaultFormats
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import spray.can.Http
import spray.http.MediaTypes._
import spray.httpx.Json4sJacksonSupport
import spray.routing.{RequestContext, Route, HttpService}

import cn.ac.ict.acs.netflow._
import cn.ac.ict.acs.netflow.util._
import cn.ac.ict.acs.netflow.master.QueryMaster

class RestBroker(
    val host: String,
    val port: Int,
    val restPort: Int,
    val masterAkkaUrls: Array[String],
    val actorSystemName: String,
    val actorName: String,
    val conf: NetFlowConf)
  extends Actor with RestService with BrokerLike with ActorLogReceive with Logging {

  implicit def actorRefFactory: ActorContext = context

  override def receiveWithLogging =
    lifecycleMaintenance orElse runRoute(routeImpl)
}

trait BrokerLike {
  this: RestBroker =>

  import context.dispatcher
  import DeployMessages._

  Utils.checkHost(host, "Expected hostname")
  assert (port > 0)

  val createTimeFormat = DateTimeFormat.forPattern("yyyyMMddHHmmss")

  // Send a heartbeat every (heartbeat timeout) / 4 milliseconds
  val HEARTBEAT_MILLIS = conf.getLong("netflow.broker.timeout", 60) * 1000 / 4

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
  val INITIAL_REGISTRATION_RETRY_INTERVAL = (math.round(10 *
    REGISTRATION_RETRY_FUZZ_MULTIPLIER)).seconds
  val PROLONGED_REGISTRATION_RETRY_INTERVAL = (math.round(60
    * REGISTRATION_RETRY_FUZZ_MULTIPLIER)).seconds

  var master: ActorSelection = null
  var masterAddress: Address = null
  var activeMasterUrl: String = ""
  var activeMasterWebUiUrl : String = ""
  @volatile var registered = false
  @volatile var connected = false

  val brokerId = generateBrokerId()

  var connectionAttemptCount = 0

  var registrationRetryTimer: Option[Cancellable] = None

  override def preStart(): Unit = {
    assert(!registered)
    logInfo(("Starting NetFlow Broker %s:%d, listening" +
      " to %d as REST Serive").format(host, port, restPort))
    logInfo(s"Running NetFlow version ${cn.ac.ict.acs.netflow.NETFLOW_VERSION}")

    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])

    registerWithMaster()
  }

  override def postStop(): Unit = {
    registrationRetryTimer.foreach(_.cancel())
  }

  def changeMaster(url: String, webUrl: String): Unit = {
    activeMasterUrl = url
    activeMasterWebUiUrl = webUrl
    master = context.actorSelection(
      QueryMaster.toAkkaUrl(activeMasterUrl, AkkaUtils.protocol(context.system)))
    masterAddress = QueryMaster.toAkkaAddress(activeMasterUrl, AkkaUtils.protocol(context.system))
    connected = true
    // Cancel any outstanding re-registration attempts because we found a new master
    registrationRetryTimer.foreach(_.cancel())
    registrationRetryTimer = None
  }

  def registerWithMaster() {
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
      actor ! RegisterBroker(brokerId, host, port, restPort)
    }
  }

  /**
   * Re-register with the master because a network failure or a master failure has occurred.
   * If the re-registration attempt threshold is exceeded, the broker exits with error.
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
          master ! RegisterBroker(brokerId, host, port, restPort)
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

  def masterDisconnected(): Unit = {
    logError("Connection to master failed! Waiting for master to reconnect...")
    connected = false
    registerWithMaster()
  }

  def generateBrokerId(): String = {
    "broker-%s-%s-%d".format((new DateTime).toString(createTimeFormat), host, port)
  }

  val lifecycleMaintenance: Receive = {
    case RegisteredBroker(masterUrl, masterWebUrl) => {
      logInfo("Successfully registered with Query Master " + masterUrl)
      registered = true
      changeMaster(masterUrl, masterWebUrl)
      context.system.scheduler.schedule(0.millis, HEARTBEAT_MILLIS.millis, self, SendHeartbeat)
    }

    case RegisterBrokerFailed(message) => {
      if (!registered) {
        logError("Worker registration failed: " + message)
        System.exit(1)
      }
    }

    case ReconnectBroker(masterUrl) => {
      logInfo(s"Master with url $masterUrl requested this worker to reconnect.")
      registerWithMaster()
    }

    case ReregisterWithMaster =>
      reregisterWithMaster()

    case MasterChanged(masterUrl, masterWebUrl) => {
      // Get this message because of master recovery
      logInfo("Master has changed, new master is at " + masterUrl)
      changeMaster(masterUrl, masterWebUrl)
      sender ! BrokerStateResponse(brokerId)
    }

    case SendHeartbeat => {
      if (connected) { master ! Heartbeat(brokerId) }
    }

    case x: DisassociatedEvent if x.remoteAddress == masterAddress => {
      logInfo(s"$x Disassociated !")
      masterDisconnected()
    }
  }
}

trait RestService extends HttpService with Json4sJacksonSupport {
  this: RestBroker =>

  import RestMessages._

  implicit def json4sJacksonFormats = DefaultFormats

  val routeImpl = respondWithMediaType(`application/json`) {
    path("status") {
      get {
        requestQueryMaster {
          RestQueryMasterStatusRequest
        }
      }
    } ~
    pathPrefix("v1" / "jobs") {
      pathEndOrSingleSlash {
        get {
          requestQueryMaster {
            RestAllJobsInfoRequest
          }
        } ~
        post {
          entity(as[JobDescription]) { jobDesc =>
            val (vjd, success, message) = jobDesc.doValidate()
            if (success) {
              requestQueryMaster {
                RestSubmitJobRequest(vjd)
              }
            } else {
              complete(400, message.getOrElse("Invalid job description"))
            }
          }
        }
      } ~
      path(Rest) { jobId =>
        get {
          requestQueryMaster {
            RestJobInfoRequest(jobId)
          }
        } ~
        delete {
          requestQueryMaster {
            RestKillJobRequest(jobId)
          }
        }
      }
    }
  }

  def requestQueryMaster(message: RestMessage): Route = {
    ctx => handleRequest(ctx, message, master)
  }

  def handleRequest(rc: RequestContext, message: RestMessage, master: ActorSelection) = {
    context.actorOf(Props(classOf[RequestHandler], rc, message, master, conf))
  }
}

object RestBroker extends Logging {
  val systemName = "netflowRest"
  val actorName = "RestBroker"

  def main(argStrings: Array[String]) {
    SignalLogger.register(log)
    val newConf = new NetFlowConf
    val args = new BrokerArguments(argStrings, newConf)
    val (actorSystem, _, _, _) = startSystemAndActor(args.host, args.port, args.restPort,
      args.masters, newConf)
    actorSystem.awaitTermination()
  }

  def startSystemAndActor(
    host: String,
    port: Int,
    restPort: Int,
    masterUrls: Array[String],
    conf: NetFlowConf = new NetFlowConf): (ActorSystem, ActorRef, Int, Int) = {

    val sysName = systemName + "_" + UUID.randomUUID()
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(sysName, host, port, conf)
    val masterAkkaUrls = masterUrls.map(
      QueryMaster.toAkkaUrl(_, AkkaUtils.protocol(actorSystem)))
    val (actor, boundRestPort) = createActorAndListen(actorSystem, host,
      boundPort, restPort, masterAkkaUrls, systemName, actorName, conf)

    (actorSystem, actor, boundPort, boundRestPort)
  }

  def createActorAndListen(
      actorSystem: ActorSystem,
      host: String,
      port: Int,
      restPort: Int,
      masterAkkaUrls: Array[String],
      actorSystemName: String,
      actorName: String,
      conf: NetFlowConf) = {
    val startActorAndListen: Int => (ActorRef, Int) = { actualRestPort =>
      doCreateActorAndListen(actorSystem, host, port, actualRestPort,
        masterAkkaUrls, actorSystemName, actorName, conf)
    }
    Utils.startServiceOnPort(restPort, startActorAndListen, conf, actorName)
  }

  def doCreateActorAndListen(
      actorSystem: ActorSystem,
      host: String,
      port: Int,
      restPort: Int,
      masterAkkaUrls: Array[String],
      actorSystemName: String,
      actorName: String,
      conf: NetFlowConf): (ActorRef, Int) = {

    val actor = actorSystem.actorOf(Props(classOf[RestBroker], host, port, restPort,
      masterAkkaUrls, actorSystemName, actorName, conf), name = actorName)

    implicitly val timeOut = AkkaUtils.askTimeout(conf)

    IO(Http) ? Http.Bind(actor, interface = host, restPort)

    (actor, restPort)
  }
}
