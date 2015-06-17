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
package cn.ac.ict.acs.netflow.load.master

import java.net.InetAddress
import java.nio.channels.SocketChannel

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random

import akka.actor._
import akka.pattern.ask
import akka.remote.RemotingLifecycleEvent
import akka.serialization.SerializationExtension

import cn.ac.ict.acs.netflow._
import cn.ac.ict.acs.netflow.load.{CombineStatus, LoadMessages}
import cn.ac.ict.acs.netflow.util._
import cn.ac.ict.acs.netflow.ha.{ LeaderElectionAgent, MonarchyLeaderAgent, LeaderElectable }

class LoadMaster(masterHost: String, masterPort: Int, webUiPort: Int, val conf: NetFlowConf)
  extends Actor with ActorLogReceive with LeaderElectable with Logging {

  import DeployMessages._
  import LoadMasterMessages._
  import LoadMessages._
  import MasterMessages._

  import context.dispatcher

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
  var state = LoadMasterRecoveryState.STANDBY
  var persistenceEngine: MasterPersistenceEngine = _
  var leaderElectionAgent: LeaderElectionAgent = _

  // load master service
  val loadServer = new MasterService(self, conf)

  /** about balance **/
  // workerIP => (IP,port)
  val workerToPort = new mutable.HashMap[String, (String, Int)]()
  // worker : buffer used rate[0,100]
  val workerToBufferRate = new mutable.HashMap[String, Double]()
  // worker : receiver => 1 : n
  val workerToReceivers = new mutable.HashMap[String, ArrayBuffer[String]]()

  // receiver : worker => 1 : n
  val receiverToWorkers = new mutable.HashMap[String, ArrayBuffer[String]]()
  // receiver => socketChannel
  val receiverToSocket = new mutable.HashMap[String, SocketChannel]()

  private val changeLimit = 0.5
  private val warnLimit = 0.7
  private val receiverConnectMaxLimit = 3   //TODO how to define thie value ? ( less than worker's number)

  /** combine parquet **/
  private var combineParquetFinished: Boolean = false

  // when there is no worker registers in cluster,
  // we put the whole request receiver into waitQueue
  val waitQueue = new mutable.HashSet[String]()

  override def preStart(): Unit = {
    logInfo(s"[Netflow] Starting NetFlow LoadMaster at $loadMasterUrl")
    logInfo(s"[Netflow] Running NetFlow version ${cn.ac.ict.acs.netflow.NETFLOW_VERSION}")

    // Listen for remote client disconnection events, since they don't go through Akka's watch()
    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])

    // TODO: a pseudo webuiurl here
    loadMasterWebUIUrl = "http://" + masterHost + ":" + webUiPort
    context.system.scheduler.schedule(0.millis, WORKER_TIMEOUT.millis, self, CheckForWorkerTimeOut)
    val (persistenceEngine_, leaderElectionAgent_) =
      RECOVERY_MODE match {
        case "ZOOKEEPER" =>
          logInfo("Persisting recovery state to ZooKeeper")
          val zkFactory =
            new ZKRecoveryModeFactory(conf, SerializationExtension(context.system))
          (zkFactory.createPersistenceEngine(), zkFactory.createLeaderElectionAgent(this))
        case _ =>
          logInfo("No state persisted as a MonarchyLeader")
          (new LoadMasterBHPersistenceEngine(), new MonarchyLeaderAgent(this))
      }
    persistenceEngine = persistenceEngine_
    leaderElectionAgent = leaderElectionAgent_

    // start the receiver master service
    loadServer.start()
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
      // TODO: dummy placeholder
      state = LoadMasterRecoveryState.ALIVE

    case RevokedLeadership => {
      logError("Leadership has been revoked -- load master shutting down.")
      System.exit(0)
    }

    /** message about maintaining the worker **/
    // include loadworker, receiver
    case RegisterWorker(id, workHost, workPort, cores, memory, webUiPort, workerIP, tcpPort) => {
      logInfo("Registering %s %s:%d with %d cores, %s RAM".format(
        id, workHost, workPort, cores, Utils.megabytesToString(memory)))

      if (state == LoadMasterRecoveryState.STANDBY) {
        // ignore, don't send response
      } else if (idToWorker.contains(id)) {
        sender ! RegisterWorkerFailed("Duplicate worker ID")
      } else {
        val worker = new LoadWorkerInfo(id, workHost, workPort, cores, memory, sender(), webUiPort)
        if (registerWorker(worker)) {
          persistenceEngine.addWorker(worker)
          sender ! RegisteredWorker(loadMasterUrl, loadMasterWebUIUrl)

          val workerIP = InetAddress.getByName(workHost).getHostAddress
          registerWorker(workerIP, tcpPort)

          pushBGPToWorker(sender())
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

      /** message about buffer **/
    case BuffersWarn(workerIp) =>
      adjustCollectorByBuffer(workerIp, sender())

    case BufferOverFlow(workerIp) =>
      adjustCollectorByBuffer(workerIp, sender())

    case BufferSimpleReport(workerIp, usageRate) =>
      workerToBufferRate.update(workerIp, usageRate)

    case BufferWholeReport(workerIp, usageRate, maxSize, curSize) =>
      workerToBufferRate.update(workerIp, usageRate)
      // TODO save another information (maxSize, curSize)

    /** message about combine **/
    // deal with the combine parquet
    case CloseParquet(fileStamp) =>
      combineParquet(fileStamp)

    case CombineFinished(status) =>
      dealWithCombineMessage(status)

      /** message about receiver **/
    case AddReceiver(receiverIP, socketChannel) =>
      registerReceiver(receiverIP, socketChannel)

    case DeleReceiver(receiverIP) =>
      deleReceiver(receiverIP)

    case DeleWorker(workerIP, port) =>
      deleWorker(workerIP, port)

    case RequestWorker(receiverIP) =>
      assignWorker(receiverIP)
  }

  // **********************************************************************************

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

    // we should tell all the receivers who connected with this
    // dead worker to connect with living worker
    adjustCollectorByDeadworker(worker.host)

    // redo combine thread on anther worker node ?
    dealWithCombineError(worker)
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

  // *********************************************************************************
  // only for combine server
  private var curComWorkerNum = 0
  private var currentCombieWorker: LoadWorkerInfo = _
  private var combineTime = new ArrayBuffer[Long]()
  private var curComidx = 0

  private def combineParquet(fileStamp: Long): Unit = {
    if(workerToPort.isEmpty) return

    curComWorkerNum match {
      case 0 =>
        val set = new mutable.HashSet[String]()
        receiverToWorkers.foreach(receiver=>{
          receiver._2.foreach(worker=>{
            set += worker
          })
        })
        curComWorkerNum = set.size
        combineTime.clear()
        combineTime += fileStamp

      case x if x == combineTime.size =>
        val ordered = combineTime.sortWith(_ < _)
        if(ordered.head != ordered.last){
          logError(s"Combine file stamp should be same, but know %s".format(ordered.mkString("<")))
          curComidx = x-1
        }

        val rdmId = Random.nextInt(idToWorker.size) // assign alive worker
        currentCombieWorker = idToWorker.toList(rdmId)._2
        currentCombieWorker.actor ! CombineParquet(combineTime(curComidx)) // tell it to combine the parquets

      case _ =>
        combineTime += fileStamp
    }
  }

  private def dealWithCombineMessage(status :CombineStatus.Value): Unit ={
    status match {
      case CombineStatus.FINISH =>
        curComWorkerNum = 0
        combineTime.clear()
        curComidx = 0
        combineParquetFinished = true

      case _ =>
        if(curComidx != 0) {
          logWarning(s"Combine error, will try another timestamp.")
          curComidx -= 1
          val rdmId = Random.nextInt(idToWorker.size) // assign alive worker
          currentCombieWorker = idToWorker.toList(rdmId)._2
          currentCombieWorker.actor ! CombineParquet(combineTime(curComidx)) // tell it to combine the parquets
        }else{
          logError(s"Combine error, cannot combine this directory")
        }
    }
  }

  /**
   * deal with the situation that
   * the worker which is running combine thread
   * id dead ,so we should redo the combine rhread on another worker node
   */
  private def dealWithCombineError(deadworker: LoadWorkerInfo): Unit = {
    if (!combineParquetFinished && deadworker == currentCombieWorker) {
      val rdmId = Random.nextInt(idToWorker.size) // assign alive worker
      currentCombieWorker = idToWorker.toList(rdmId)._2
      currentCombieWorker.actor ! CombineParquet(combineTime(curComidx)) // tell it to combine the parquets
    }
  }

  // ***********************************************************************************
  // only for bgp table
  private val bgpTable = new scala.collection.mutable.HashMap[Int, Array[Byte]]
  private def updateBGP(bgpIds: Array[Int], bgpDatas: Array[Array[Byte]]): Unit ={

    if(bgpTable.isEmpty){
      idToWorker.valuesIterator
        .foreach(_.actor! updateBGP(bgpIds, bgpDatas))

      var idx = 0
      while(idx != bgpIds.length){
        bgpTable(bgpIds(idx)) = bgpDatas(idx)
        idx += 1
      }
      return
    }

    // update exist table
    val _bgpIds = new ArrayBuffer[Int]
    val _bgpDatas = new ArrayBuffer[Array[Byte]]

    var idx = 0
    while(idx != bgpIds.length){
      if(bgpTable.contains(bgpIds(idx))){
        val value = bgpTable.get(bgpIds(idx)).get
        if(!(value sameElements bgpDatas(idx))){
          _bgpIds += bgpIds(idx)
          _bgpDatas += bgpDatas(idx)
          bgpTable(bgpIds(idx)) = bgpDatas(idx)
        }
      }else{
        _bgpIds += bgpIds(idx)
        _bgpDatas += bgpDatas(idx)
        bgpTable(bgpIds(idx)) = bgpDatas(idx)
      }
      idx += 1
    }
    idToWorker.valuesIterator
      .foreach(_.actor! updateBGP(_bgpIds.toArray[Int],_bgpDatas.toArray[Array[Byte]]))

  }

  private def pushBGPToWorker(workerActor: ActorRef): Unit = {
    if (bgpTable.isEmpty) return

    val bgpIds = new Array[Int](bgpTable.size)
    val bgpDatas = new Array[Array[Byte]](bgpTable.size)
    var idx = 0
    bgpTable.foreach(record => {
      bgpIds(idx) = record._1
      bgpDatas(idx) = record._2
      idx += 1
    })
    workerActor ! updateBGP(bgpIds, bgpDatas)
  }


  // ***********************************************************************************
  /** deal with worker **/
  // called after a worker registered successfully, record workerToUdpPort & workerToCollectors
  private def registerWorker(workerIP: String, workerPort: Int): Unit = {
    workerToPort += (workerIP -> (workerIP, workerPort))
    workerToReceivers += (workerIP -> new ArrayBuffer[String])
    workerToBufferRate += (workerIP -> 0)
    assignWorkerToWaittingReceiver(workerIP)
  }

  private def deleWorker(workerIP: String, port: Int): Unit = {
    if (workerToPort.contains(workerIP) && workerToPort(workerIP)._2 == port) {
      workerToPort -= workerIP
      workerToBufferRate -= workerIP
      workerToReceivers.remove(workerIP) match {
        case Some(recvs) =>
          recvs.foreach(recv=> {
            val workers = receiverToWorkers(recv)
            workers.remove(workers.indexOf(workerIP))
          })

        case None =>
      }
    }
  }

  // when a worker registered in master, select a receiver to connect with this worker
  private def assignWorkerToWaittingReceiver(workerIP: String): Unit =  {
    if (waitQueue.nonEmpty) {
      val cmd = CommandSet.responseReceiver(Some(Array(workerToPort.get(workerIP).get)), None)

      // we want to select the same node firstly
      val receiver = if(waitQueue.contains(workerIP)) workerIP else waitQueue.head

      val receiverSocket = receiverToSocket(receiver)  // error not found key
      receiverSocket.write(cmd)
      waitQueue.remove(workerIP)
    }
  }

  // ***********************************************************************************
  /** deal with receiver **/
  // the method is called only by first connection for receiverIP
  private def registerReceiver(receiverIP: String, socketChannel: SocketChannel) = {
    if (receiverToSocket.contains(receiverIP)) {
      logError(s"[Netflow] The receiver $receiverIP should not exist in receiverToSocket !")
    }
    if (receiverToWorkers.contains(receiverIP)) {
      logError(s"[Netflow] The receiver $receiverIP should not exist in receiverToWorkers !")
    }

    receiverToSocket += (receiverIP -> socketChannel)
    receiverToWorkers += (receiverIP -> new ArrayBuffer[String]())
  }

  private def deleReceiver(receiverIP: String): Unit ={
    receiverToSocket -= receiverIP
    receiverToWorkers.remove(receiverIP) match {
      case Some(_workers) =>
        _workers.foreach(_worker=>{
          val _receivers = workerToReceivers(_worker)
          _receivers.remove(_receivers.indexOf(receiverIP))
          })

      case None =>
    }
  }


  // called when a receiver ask for worker's info
  private def assignWorker(receiver: String, workerNum: Int = 1): Unit = {

    receiverToSocket.get(receiver) match {
      case Some(socket) =>

        receiverToWorkers.get(receiver) match {
          case None =>
            logError(s"[ Netflow ] The node $receiver should be registered! ")
            waitQueue += receiver

          case Some(_workers) =>
            if (_workers.isEmpty) {
              // the first time to assign worker

              if (workerToPort.isEmpty) {
                logWarning(s"[Netflow] There is no available worker to run in cluster.")
                waitQueue += receiver
                return
              }

              // first, try to assign itself
              if (!workerToPort.contains(receiver)) Thread.sleep(2000)
              if (workerToPort.contains(receiver)) {
                _workers += receiver
                workerToReceivers(receiver) += receiver
              }
              if(_workers.size == workerNum) return


              // if needs more than one worker
              val orderByworkerList =
                workerToBufferRate.filterNot(x => x._1 == receiver).toList.sortWith(_._2 < _._2)

              var oi = 0
              while (_workers.size != workerNum && oi < orderByworkerList.size){
                val _worker = orderByworkerList(oi)._1
                _workers += _worker
              workerToReceivers(_worker) += receiver
              oi += 1
              }
            }

            val workerList = new Array[(String, Int)](workerNum)
            for (i <- _workers.indices) {
              workerList(i) = workerToPort.get(_workers(i)).get
            }

            val cmd = CommandSet.responseReceiver(Some(workerList), None)
            receiverToSocket(receiver).write(cmd)
        }
      case None =>
    }
  }


  // ***********************************************************************************

  /** deal with balance **/

  private def getAllWorkerBufferRate() = {
    idToWorker.values.foreach(x => x.actor! BufferInfo)
  }

  // notify receiver to change worker
  private def notifyReceiver(receiverHost: String,
                             addHost: Option[Array[(String, Int)]],
                             deleHost: Option[Array[(String, Int)]]): Unit = {

    val res = CommandSet.responseReceiver(addHost,deleHost)
    receiverToSocket.get(receiverHost) match {
      case None =>
        logError("[Netflow] receiver lost!")

      case Some(socket) =>
        if (socket.isConnected) {
          socket.write(res)
        } else {
          logError(s"[Netfow] Can not connect with $receiverHost receiver")
        }
    }
  }

  private def adjustCollectorByDeadworker(deadworkerIP: String): Unit = {
    getAllWorkerBufferRate()

    workerToPort -= deadworkerIP
    workerToBufferRate -= deadworkerIP

    workerToReceivers.remove(deadworkerIP) match {

      case Some(receivers) =>
        receivers.foreach(f = receiver => {
          // delete deadworker first
          receiverToWorkers.get(receiver).get -= deadworkerIP

          val availableWorker = workerToBufferRate.toList.sortWith(_._2 < _._2)
          if (availableWorker.isEmpty) {
            logError("[Netflow]No available worker to worker.")
            return
          }

          val workers = receiverToWorkers.get(receiver).get

          if (workers.isEmpty) {
            // only one worker to connect this receiver, so we should
            // tell the receiver to connect another worker

            var addWorkers: Array[(String, Int)] = null

            if (availableWorker.head._2 < changeLimit) {
              // we believe that a single worker can deal with this receiver

              workerToPort.get(availableWorker.head._1) match{
                case Some(a_worker) =>
                  addWorkers = Array(a_worker)
                case None => logError(s"${availableWorker.head._1} lost ... current size : ${workerToPort.size}")
              }
              addWorkers = Array(workerToPort.get(availableWorker.head._1).get)
            } else {
              // a worker can not competent at this job....
              // so amortize job to more than one worker
              val maxNum = math.min(receiverConnectMaxLimit, availableWorker.size)

              addWorkers = new Array[(String, Int)](maxNum)
              var i = 0
              availableWorker.take(maxNum).foreach(x => {
                addWorkers(i) = workerToPort.get(x._1).get
                i += 1
              })
            }
            notifyReceiver(receiver, Some(addWorkers), Some(Array((deadworkerIP, 0))))
          } else {
            // more than one worker to connect this receiver,
            // so we should adjust whether it's worker buffer available is more than 50% ?

            val connectedWorker = availableWorker.filter(x => workers.contains(x._1))
            if (connectedWorker.last._2 < changeLimit) {
              // all connection's worker buffer < 50%, we believe that,
              // deleting a worker dose not effect on current system
              notifyReceiver(receiver, None, Some(Array((deadworkerIP, 0))))
            } else {
              // we may amortize job to another workers
              val dicConnectWorker = availableWorker.filterNot(x => workers.contains(x._1))
              notifyReceiver(receiver,
                Some(Array(workerToPort.get(dicConnectWorker.head._1).get)),
                Some(Array((deadworkerIP, 0))))
            }
          }
        })

      case None =>
        logError("The worker %s should be connect with at lest one receiver, but now empty"
          .format(deadworkerIP))
    }
  }

  // Called when a worker need to adjust receiver number
  // leave the receiver co-located with worker to be the last one to remove
  private def adjustCollectorByBuffer(workerHost: String, workerActor: ActorRef): Unit = {
    getAllWorkerBufferRate()

    // tell the receivers that connects with this worker to connect another worker
    workerToReceivers.get(workerHost) match {
      case Some(receiverList) =>
        if (receiverList.isEmpty) {
          logError(s"[Netflow] The $workerHost load worker's receiver should not be empty.")
          return
        }

        logInfo {s"[Netflow] $workerHost load worker has ${receiverList.size} receivers" +
            s" (${receiverList.mkString(" ")}) to connect with."}

        if (receiverList.size == 1) {
          // only one receiver connect with this worker , what should we do?

          val receiver = receiverList.head
          val workers = receiverToWorkers.get(receiver).get

          logInfo(s"[Netflow] The load worker only has one receiver $receiver," +
            s"and this receiver is connecting with ${workers.size} workers(${workers.mkString(" ")}).")

          val availableWorkers = workerToBufferRate
            .filterNot(x => workers.contains(x)).toList.sortWith(_._2 < _._2)

          if (availableWorkers.isEmpty) {
            logInfo(String.format(
              "[Netflow] Total worker number is %d (%s)," +
                "which are used by %s (%s) receiver," +
                """so there is no available worker to adjust.""" +
                "Only to increase worker's thread.",
              workerToPort.size: Integer,
              workerToPort.keys.mkString(" "),
              receiver,
              workers.mkString(" ")))

            workerActor ! AdjustThread
            return
          }

          var idx = 0
          var flag = true
          while (availableWorkers(idx)._2 < changeLimit && flag) {

            val targetWorker = availableWorkers(idx)._1
            val targetReceiverlist = workerToReceivers.get(targetWorker).get

            logInfo(s"[Netflow] Target $idx worker is $targetWorker " +
              s"and its buffer rate is ${availableWorkers(idx)._2}." +
              s"Its connecting receiver is ${targetReceiverlist.mkString(" ")}.")

            if (!targetReceiverlist.contains(receiver)) {
              notifyReceiver(receiver, Some(Array(workerToPort.get(targetWorker).get)), None)
              workerToReceivers.get(targetWorker).get += receiver
              receiverToWorkers.get(receiver).get += targetWorker

              flag = false
              logInfo(s"[Netflow] As currnet receiver is not in target worker's receivering," +
                s"So we notify $receive receiver to add $targetWorker worker.")
            } else {
              idx += 1
              logInfo("[Netflow] As current receiver is in target worker's receiver list," +
                "So we should find another worker.")
            }
          }

          logInfo(s"[Netflow] Current idx $idx, total available worker is ${availableWorkers.size}.")

          if (flag) {
            // there is no available worker to meet current rate < changeLimit
            val maxNum = math.min(availableWorkers.size, receiverConnectMaxLimit)
            val targetWorker = new ArrayBuffer[(String, Int)](maxNum)

            var rate = availableWorkers(idx)._2
            var i = 0
            while (rate < warnLimit && i != maxNum) {
              val work = availableWorkers(i)
              targetWorker += workerToPort.get(work._1).get
              i += 1
              rate = availableWorkers(i)._2
              logInfo(s"[Netflow] Current available worker is ${work._1} and its buffer rate is ${work._2}.")
            }

            // all worker's buffer are > 0.7
            if (targetWorker.isEmpty) {
              logInfo(s"[Netflow] There is no buffer userage rate that < 0.7.")

              for (i <- 0 until maxNum) {
                targetWorker += workerToPort.get(availableWorkers(i)._1).get
              }
            }

            notifyReceiver(receiver, Some(targetWorker.toArray), None)
            targetWorker.foreach(x => {
              workerToReceivers.get(x._1).get += receiver
              receiverToWorkers.get(receiver).get += x._1
            })
          }

        } else {
          // more than one receiver
          val availableWorkerList =
            workerToBufferRate.filterNot(x => x._1 == workerHost).toList.sortWith(_._2 < _._2)

          if (availableWorkerList.isEmpty) {
            logWarning("[Netflow] Threr is no available worker to used. ")
            workerActor ! AdjustThread
          }

          var idx = 0
          var flag = true
          while (availableWorkerList(idx)._2 < changeLimit && flag) {
            val targetWorker = availableWorkerList(idx)._1
            val targetReceiverlist = workerToReceivers.get(targetWorker).get
                .filterNot(x => receiverList.contains(x))
            if (targetReceiverlist.nonEmpty) {
              // we find a receiver to meet our needs

              val targetReceiver = targetReceiverlist.head

              notifyReceiver(targetReceiver,
                Some(Array(workerToPort.get(targetWorker).get)),
                Some(Array(workerToPort.get(workerHost).get)))

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
              if (_availableWorkerList.nonEmpty) {
                val _targetWorker = _availableWorkerList.head._1
                notifyReceiver(r, Some(Array(workerToPort.get(_targetWorker).get)), None)
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

}

object LoadMaster extends Logging {

  import MasterMessages._

  val systemName = "netflowLoadMaster"
  private val actorName = "LoadMaster"

  def main(argStrings: Array[String]): Unit = {
    SignalLogger.register(log)
    val conf = new NetFlowConf(false)
    val masterArg = new LoadMasterArguments(argStrings, conf)
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
      Props(classOf[LoadMaster], host, boundPort, webUiPort, conf), actorName)
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
}
