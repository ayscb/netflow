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
package cn.ac.ict.acs.netflow.load2.deploy.loadDeploy

import java.nio.channels.{ SocketChannel}

import akka.actor._
import akka.pattern.ask
import akka.remote.RemotingLifecycleEvent
import akka.serialization.SerializationExtension
import cn.ac.ict.acs.netflow.load2.deploy.DeployMessages._
import cn.ac.ict.acs.netflow.load2.deploy.LoadMasterMessage.{BufferInfo, AdjustThread, NeedToCombineParquet, CombineParquet}
import cn.ac.ict.acs.netflow.load2.deploy.LoadWorkerMessage.{BufferReport, CombineFinished, BufferOverFlow, BuffersWarn}
import cn.ac.ict.acs.netflow.load2.deploy.MasterMessage._
import cn.ac.ict.acs.netflow.load2.deploy.recovery._
import cn.ac.ict.acs.netflow.load2.deploy._
import cn.ac.ict.acs.netflow.util.{AkkaUtils, SignalLogger, ActorLogReceive, Utils}

import cn.ac.ict.acs.netflow.{IPv4, Logging, NetFlowConf, NetFlowException}
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random

/**
 * Created by ayscb on 2015/4/28.
 */
class LoadMaster(
                  masterHost: String,
                  masterPort: Int,
                  webUiPort: Int,
                  val conf: NetFlowConf)
extends Actor with ActorLogReceive with MasterService with LeaderElectable with Logging {

  val createTimeFormat = DateTimeFormat.forPattern("yyyyMMddHHmmss")

  // the interval for checking workes and receivers
  val WORKER_TIMEOUT = conf.getLong("netflow.LoadWorker.timeout", 60) * 1000

  // Remove a dead worker after given interval
  val REAPER_ITERATIONS = conf.getInt("netflow.dead.worker.persistence", 15)

  // Master recovery mode
  val RECOVERY_MODE = conf.get("netflow.deploy.recoveryMode", "NONE")

  // This may contain dead workers and receivers
  val workers = new mutable.HashSet[LoadWorkerInfo]

  // Current alive workers and receivers
  val idToWorker = new mutable.HashMap[String, LoadWorkerInfo]

  // Current alive workers and receivers
  val addressToWorker = new mutable.HashMap[Address, LoadWorkerInfo]

  Utils.checkHost(masterHost, "Expected hostname")

  val loadMasterUrl = "netflow-load://" + masterHost + ":" + masterPort
  var loadMasterWebUIUrl: String = _

  var masterState = MasterRecoveryState.STANDBY

  var persistenceEngine: PersistenceEngine = _
  var leaderElectionAgent: LeaderElectionAgent = _

  val workerToPort = new mutable.HashMap[String,(String,Int)]()   // workerIP => (IP,port)
  val receiverToSocket = new mutable.HashMap[String,SocketChannel]()  //receiver => socketChannel

  val workerToReceivers = new mutable.HashMap[String,ArrayBuffer[String]]()   //worker : receiver => 1 : n
  val receiverToWorkers = new mutable.HashMap[String,ArrayBuffer[String]]()   //receiver : worker => 1 : n

  val workerToBufferRate = new mutable.HashMap[String,Int]()  // worker : buffer used rate[0,100]

  import scala.concurrent.ExecutionContext.Implicits.global

  override def preStart(): Unit = {
    logInfo(s"[ netflow ] Starting NetFlow LoadMaster at $loadMasterUrl" )
    logInfo(s"[ netflow ] Running NetFlow version ${cn.ac.ict.acs.netflow.NETFLOW_VERSION}")

    // Listen for remote client disconnection events, since they don't go through Akka's watch()
    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])

    //TODO: a pseudo webuiurl here
    loadMasterWebUIUrl = "http://" + masterHost + ":" + webUiPort
    context.system.scheduler.schedule(0.millis, WORKER_TIMEOUT.millis, self, CheckForWorkerTimeOut)
    val (persistenceEngine_, leaderElectionAgent_) =
      RECOVERY_MODE match {
        case "ZOOKEEPER" =>
          logInfo("Persisting recovery state to ZooKeeper")
          val zkFactory =
          new ZooKeeperRecoveryModeFactory(conf, SerializationExtension(context.system))
          (zkFactory.createPersistenceEngine(), zkFactory.createLeaderElectionAgent(this))

        case "FILESYSTEM" =>
          val fsFactory =
            new FileSystemRecoveryModeFactory(conf, SerializationExtension(context.system))
          (fsFactory.createPersistenceEngine(), fsFactory.createLeaderElectionAgent(this))

        case _ =>
          logInfo("No state persisted as a MonarchyLeader")
          val MonarchyFactory =
          new MonarchyRecoveryModeFactory()
          (MonarchyFactory.createPersistenceEngine(),MonarchyFactory.createLeaderElectionAgent(this))
      }
    persistenceEngine = persistenceEngine_
    leaderElectionAgent = leaderElectionAgent_

    // start the receiver master service
    startThread()

    //start the combine scheduler
    val interval = conf.doctTimeIntervalValue
    context.system.scheduler.schedule(0.millis,interval.seconds,self,NeedToCombineParquet)
  }

  override def preRestart(reason: Throwable, message: Option[Any]) {
    super.preRestart(reason, message) // calls postStop()!
    logError("LoadMaster actor restarted due to exception", reason)
  }

  override def postStop(): Unit = {
    persistenceEngine.close()
    leaderElectionAgent.stop()
  }

  override def appointLeader() = {
    self ! AppointedAsLeader
  }

  override def revokeLeadership() = {
    self ! RevokedLeadership
  }

  override def receiveWithLogging: PartialFunction[Any, Unit] = {

    case AppointedAsLeader =>
      //TODO: dummy placeholder
      masterState = MasterRecoveryState.ALIVE

    case RevokedLeadership => {
      logError("Leadership has been revoked -- load master shutting down.")
      System.exit(0)
    }

      // include loadworker, receiver
    case RegisterWorker(id, workHost, workPort, cores, memory, webUiPort,tcpPort) => {
      logInfo("Registering %s %s:%d with %d cores, %s RAM".format(
        id, workHost, workPort, cores, Utils.megabytesToString(memory)))

      if (masterState == MasterRecoveryState.STANDBY) {
        // ignore, don't send response
      } else if (idToWorker.contains(id)) {
        sender ! RegisterWorkerFailed("Duplicate worker ID")
      } else {
        val worker = new LoadWorkerInfo(id, workHost, workPort, cores, memory, sender, webUiPort, tcpPort)
        if (registerWorker(worker)) {
          persistenceEngine.addWorker(worker)
          sender ! RegisteredWorker(loadMasterUrl, loadMasterWebUIUrl)

          registerWorkerStructor(workHost,tcpPort)
        } else {
          val workerAddress = worker.actor.path.address
          logWarning("Worker registration failed. Attempted to re-register worker at same " +
            "address: " + workerAddress)
          sender ! RegisterWorkerFailed("Attempted to re-register Component at same address: "
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
                logWarning(s"Got heartbeat from unregistered component $workerId." +
                  " Asking it to re-register.")
                sender ! ReconnectWorker(loadMasterUrl)
              } else {
                logWarning(s"Got heartbeat from unregistered worker $workerId." +
                  " This worker was never registered, so ignoring the heartbeat.")
              }
          }

    case CheckForWorkerTimeOut =>
      timeOutDeadWorkers()

    case BoundPortsRequest =>
      sender ! BoundPortsResponse(masterPort, webUiPort)

    // when receive this message, that means only by increasing worker's writer thread do not
    case BuffersWarn (workerHost) =>
      adjustCollectorByBuffer(workerHost,sender())

    case BufferOverFlow (workerHost) =>
      adjustCollectorByBuffer(workerHost,sender())

    case BufferReport(workerHost,rate) =>
        workerToBufferRate.update(workerHost,rate)

      // deal with the combine parquet
    case NeedToCombineParquet => combineParquet()

    case CombineFinished => combineParquetFinished = true

  }

  private def registerWorker(worker: LoadWorkerInfo): Boolean = {
    // There may be one or more refs to dead workers on this same node (with different ID's),
    // remove them.
    workers.filter { w =>
      (w.host == worker.host && w.port == worker.port) && (w.state == WorkerState.DEAD)
    }.foreach { w =>
      workers -= w
    }

    val workerAddress = worker.actor.path.address
    if (addressToWorker.contains(workerAddress)) {
      val oldWorker = addressToWorker(workerAddress)
      if (oldWorker.state == WorkerState.UNKNOWN) {
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

  private def removeWorker(worker: LoadWorkerInfo): Unit = {
    logInfo("Removing worker " + worker.id + " on " + worker.host + ":" + worker.port)
    worker.setState(WorkerState.DEAD)
    idToWorker -= worker.id
    addressToWorker -= worker.actor.path.address
    persistenceEngine.removeWorker(worker)

    // we should tell all the receivers who connected with this dead worker to connect with living worker
    adjustCollectorByDeadworker(worker.host)

    // redo combine thread on anther worker node ?
    dealWithCombineError()
  }

  /** Check for, and remove, any timed-out workers */
  private def timeOutDeadWorkers() {
    // Copy the workers into an array so we don't modify the hashset while iterating through it
    val currentTime = System.currentTimeMillis()
    val toRemove = workers.filter(_.lastHeartbeat < currentTime - WORKER_TIMEOUT).toArray
    for (worker <- toRemove) {
      if (worker.state != WorkerState.DEAD) {
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

  private var combineParquetFinished : Boolean = false
  private def combineParquet(): Unit ={
      val rdmId = Random.nextInt(idToWorker.size)     // assign alive worker
      val workInfo = idToWorker.toList(rdmId)._2
      workInfo.actor ! CombineParquet                 // tell it to combine the parquets
  }

  /**
   * deal with the situation that
   * the worker which is running combine thread
   * id dead ,so we should redo the combinr rhread on another worker node
   */
  private def dealWithCombineError(): Unit={
    if(!combineParquetFinished)
      combineParquet()
  }

  //**********************************************************************************
import netUtil.Mode._

  private def getWholeInfo: Unit ={
    idToWorker.values.foreach(x=>x.actor ! BufferInfo)
  }

  // 通知receiver 该修改worker 了
  private def notifyReceiver(
                              mode:Mode,
                              receiverHost:String,
                              newHost:Array[(String,Int)]):Boolean ={
      val res = netUtil.responseReceiver(mode,newHost)
      receiverToSocket.get(receiverHost) match {
        case None =>
          logError("[Netflow] receiver lost!")
          false
        case Some(socket) =>
          if(socket.isConnected){
            socket.write(res)
            true
          }else{
            logError(s"[Netfow] Can not connect with $receiverHost receiver")
            false
          }
      }
  }

  private val changeLimit = 50
  private val warnLimit = 70
  private val receiverConnectMaxLimit = 3   // should be < worker's number

  // 这个方法在worker 失效的时候调用 ，
  // 需要动态的修改 workerTotcpPort, workerToReceivers, receiverToworkers
  private def adjustCollectorByDeadworker(deadworker : String): Unit ={
    getWholeInfo
    workerToPort -= deadworker
    workerToBufferRate -= deadworker

    workerToReceivers.remove(deadworker) match {
      case Some( receivers ) =>
        receivers.foreach(receiver => {
          // delete deadworker first
          receiverToWorkers.get(receiver).get -= deadworker

          val workers = receiverToWorkers.get(receiver).get
          if (workers.size == 0) {
            //only one worker to connect this receiver, so we should tell the receiver to connect another worker
            val availableWorker = workerToBufferRate.toList.sortWith(_._2 < _._2)
            if( availableWorker.size == 0){
              logError("[Netflow]No available worker to worker.")
              return
            }
            // delete the bad connection
            notifyReceiver(delete, receiver, Array((deadworker,0)))
            if( availableWorker.head._2 < changeLimit ){
              // we may believe that a worker is competent at this job!
              notifyReceiver(add, receiver, Array(workerToPort.get(availableWorker.head._1).get))
            }else{
              // a worker can not competent at this job....
              val maxNum = math.min(receiverConnectMaxLimit,availableWorker.size)
              val newAvailableWorkers = new Array[(String,Int)](maxNum)
              var i =0
              availableWorker.take(maxNum).foreach(x=>{
                newAvailableWorkers(i) = workerToPort.get(x._1).get
                i += 1
              })
              notifyReceiver(add, receiver, newAvailableWorkers)
            }
          }else{
            //more than one worker to connect this receiver,
            // so we should adjust whether it's worker buffer available is more than 50% ?
            val availableWorker = workerToBufferRate.toList.sortWith(_._2 < _._2)

            val connectedWorker = availableWorker.filter(x => workers.contains(x._1))
            if (connectedWorker.last._2 < changeLimit) {
              // all connection's worker buffer < 50
              notifyReceiver(delete, receiver, Array((deadworker,0)) )
            } else {
              val dicConnectWorker = availableWorker.filterNot(x => workers.contains(x._1))
              notifyReceiver(delete, receiver, Array((deadworker,0)))
              notifyReceiver(add, receiver, Array(workerToPort.get(dicConnectWorker.head._1).get))
            }
          }
        })

      case None =>
        logError("The worker %s should be connect with at lest one receiver, but now empty"
          .format(deadworker))
    }
  }

  // 这个方法在收到worker 要调节receiver的时候调用，主要来减少当前worker连接的receiver数目
  // 最后留下的那个receiver 必须 和worker 的host 地址一样 （ 如果receiver和worker有相同的host的时候）
  private def adjustCollectorByBuffer(workerHost:String, workerActor:ActorRef): Unit={
    getWholeInfo

    // tell the receivers who connects with this worker to connect another worker
    workerToReceivers.get(workerHost) match {
      case Some(receiverList) =>
        if( receiverList.size == 0){
          logError(s"[Netflow] The $workerHost load worker's receiver should not be empty.")
          return
        }

        logInfo(
          String.format(
            "[Netflow] %s load worker has %d receivers (%s) to connect with.",
            workerHost,receiverList.size, receiverList.mkString(" ")))

        if( receiverList.size == 1) {
          // we can not adjust the receiver
          val receiver = receiverList.head
          val workers = receiverToWorkers.get(receiver).get

          logInfo(String.format(
          "[Netflow] The load worker only has one receiver %s," +
            "and this receiver is connecting with %d workers(%s).",
            receiver,workers.size,workers.mkString(" ")
          ))

          val availableWorkers
            = workerToBufferRate.filterNot(x=>workers.contains(x))
                .toList.sortWith(_._2<_._2)

          if( availableWorkers.size == 0){
            logInfo(String.format(
            "[Netflow] Total worker number is %d (%s)," +
              "which are used by %s (%s) receiver," +
              "so there is no available worker to adjust." +
              "Only to increase worker's thread.",
              workerToPort.size,
              workerToPort.map(x=>x._1).mkString(" "),
              receiver,
              workers.mkString(" ")))
            workerActor ! AdjustThread
          }else{

            var idx = 0
            var flag = true
            while (availableWorkers(idx)._2 < changeLimit && flag) {
              val targetWorker = availableWorkers(idx)._1
              val targetReceiverlist =
                workerToReceivers.get(targetWorker).get

              logInfo(String.format(
                "[Netflow] Target %d worker is %s and its buffer rate is %d. " +
                  "Its connecting receiver is %s.",
                idx,targetWorker,availableWorkers(idx)._2,
                targetReceiverlist.mkString(" ")))

              if (!targetReceiverlist.contains(receiver)) {
                notifyReceiver(add, receiver, Array(workerToPort.get(targetWorker).get))
                workerToReceivers.get(targetWorker).get += receiver
                receiverToWorkers.get(receiver).get += targetWorker

                flag = false
                logInfo(String.format(
                  "[Netflow] As currnet receiver is not in target worker's receivering," +
                  "So we notify %s receiver to add %s worker",
                  receiver,targetWorker))
              } else {
                idx += 1
                logInfo(String.format(
                  "[Netflow] As current receiver is in target worker's receiver list," +
                    "So we should find another worker."))
              }
            }

            logInfo(String.format(
              "[Netflow] Current idx = %d, total available worker is %d.",
              idx,availableWorkers.size))

            if( flag ){
              // there is no available worker to meet the
              val maxNum = math.min(availableWorkers.size,receiverConnectMaxLimit)
              val targetWorker = new ArrayBuffer[(String,Int)](maxNum)

              var rate = availableWorkers(idx)._2
              var i = 0
              while ( rate < warnLimit && i != maxNum){
                val work = availableWorkers(i)
                targetWorker += workerToPort.get(work._1).get
                i += 1
                rate = availableWorkers(i)._2
                logInfo(String.format(
                  "[Netflow] Current available worker is %s and its buffer rate is %d.",
                  work,work._2))
              }

              // all worker's buffer are > 70
              if( targetWorker.size == 0){
                logInfo(String.format(
                  "[Netflow] There is no buffer rate that < 70."))

                for( i <- 0 until maxNum){
                  targetWorker += workerToPort.get(availableWorkers(i)._1).get
                }
              }

              notifyReceiver(add,receiver,targetWorker.toArray)
              targetWorker.foreach(x=>{
                workerToReceivers.get(x._1).get += receiver
                receiverToWorkers.get(receiver) += x._1
              })
            }
          }
        }else {
          // more than one receiver
          val availableWorkerList: List[(String, Int)]
          = workerToBufferRate.filterNot(x => x._1 == workerHost)
            .toList.sortWith(_._2 < _._2)
          if (availableWorkerList.size == 0) {
            logWarning("[Netflow] Threr is no available worker to used. ")
            workerActor ! AdjustThread
          }

          var idx = 0
          var flag = true
          while (availableWorkerList(idx)._2 < changeLimit && flag) {
            val targetWorker = availableWorkerList(idx)._1
            val targetReceiverlist =
              workerToReceivers.get(targetWorker).get
                .filterNot(x => receiverList.contains(x))
            if (targetReceiverlist.size != 0) {
              // we find a receiver to meet our needs
              val targetReceiver = targetReceiverlist.head
              notifyReceiver(delete, targetReceiver, Array(workerToPort.get(workerHost).get))
              notifyReceiver(add, targetReceiver, Array(workerToPort.get(targetWorker).get))

              workerToReceivers.get(workerHost).get -= targetReceiver
              workerToReceivers.get(targetWorker).get += targetReceiver
              receiverToWorkers.get(targetReceiver).get -= workerHost
              receiverToWorkers.get(targetReceiver).get += targetWorker
              flag = false
            } else {
              idx += 1
            }
          }

          if (flag) {
            // that is the availableWorkerList > 50
            receiverList.foreach(r => {
              val _workerList = receiverToWorkers.get(r).get
              val _availableWorkerList =
                workerToBufferRate.filterNot(x => _workerList.contains(x))
                  .toList.sortWith(_._2 < _._2)
              if (_availableWorkerList.size != 0) {
                val _targetWorker = _availableWorkerList.head._1
                notifyReceiver(add, r, Array(workerToPort.get(_targetWorker).get))
                workerToReceivers.get(_targetWorker).get += r
                receiverToWorkers.get(r).get += _targetWorker
              }
            })
          }
        }
      case None =>
        logError(s"the worker $workerHost is not running? there are no receivers to connect with? ")
    }
  }

  // 这个方法应该在worker注册成功后调用，记录 workerToUdpPort 和 workerToCollectors
  private def registerWorkerStructor(workerHost : String, workerPort : Int ): Unit = {
    workerToPort += ( workerHost->(workerHost,workerPort) )
    workerToReceivers += ( workerHost -> new ArrayBuffer[String])
  }
}

object LoadMaster extends Logging {
  val systemName = "netflowLoadMaster"
  private val actorName = "LoadMaster"

  def main(argStrings: Array[String]): Unit = {
    SignalLogger.register(log)
    val conf = new NetFlowConf
    val masterArg = new LoadMasterArguments(argStrings, conf)
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
  def toAkkaUrl(netflowUrl: String, protocol: String): String = {
    val uri = new java.net.URI(netflowUrl)
    val host = uri.getHost
    val port = uri.getPort
    AkkaUtils.address(protocol, systemName, host, port, actorName)
  }

  /**
   * Returns an akka `Address` for the Master actor given a
   * netflowkUrl `netflow-query://host:port`.
   *
   * @throws NetFlowException if the url is invalid
   */
  def toAkkaAddress(netflowUrl: String, protocol: String): Address = {
    val uri = new java.net.URI(netflowUrl)
    val host = uri.getHost
    val port = uri.getPort
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
      Props(classOf[LoadMaster], host, boundPort, webUiPort, conf), actorName)
    val timeout = AkkaUtils.askTimeout(conf)
    val portsRequest = actor.ask(BoundPortsRequest)(timeout)
    val portsResponse = Await.result(portsRequest, timeout).asInstanceOf[BoundPortsResponse]
    (actorSystem, boundPort, portsResponse.webUIPort)
  }
}
