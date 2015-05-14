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
import java.nio.channels.{ServerSocketChannel, SelectionKey, Selector, SocketChannel}

import akka.actor._
import akka.pattern.ask
import akka.remote.RemotingLifecycleEvent
import akka.serialization.SerializationExtension
import cn.ac.ict.acs.netflow.load2.deploy.DeployMessages._
import cn.ac.ict.acs.netflow.load2.deploy.LoadMasterMessage.{NeedToCombineParquet, CombineParquet}
import cn.ac.ict.acs.netflow.load2.deploy.LoadWorkerMessage.{CombineFinished, AdjustThread, BufferOverFlow, BuffersWarn}
import cn.ac.ict.acs.netflow.load2.deploy.MasterMessage._
import cn.ac.ict.acs.netflow.load2.deploy.Recovery._
import cn.ac.ict.acs.netflow.load2.deploy._
import cn.ac.ict.acs.netflow.util._
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
//case class Collector( collHost:String, port:Int)

class LoadMaster(
                  masterHost: String,
                  masterPort: Int,
                  webUiPort: Int,
                  val conf: NetFlowConf)
extends Actor with ActorLogReceive with CollectorService with LeaderElectable with Logging {

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

  val workerToUdpPort = new mutable.HashMap[String,Int]()   // worker => udpPort
  val collectorToSocket = new mutable.HashMap[String,SocketChannel]()  //collector => socketChannel

  val workerToCollectors = new mutable.HashMap[String,ArrayBuffer[String]]()   //worker : collector => 1 : n
  val collectorToWorkers = new mutable.HashMap[String,ArrayBuffer[String]]()   //collector : worker => 1 : n

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

    // start the collect service
    collectorService.start()

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
    case RegisterWorker(id, workHost, workPort, cores, memory, webUiPort,udpPort) => {
      logInfo("Registering %s %s:%d with %d cores, %s RAM".format(
        id, workHost, workPort, cores, Utils.megabytesToString(memory)))

      if (masterState == MasterRecoveryState.STANDBY) {
        // ignore, don't send response
      } else if (idToWorker.contains(id)) {
        sender ! RegisterWorkerFailed("Duplicate worker ID")
      } else {
        val worker = new LoadWorkerInfo(id, workHost, workPort, cores, memory, sender, webUiPort, udpPort)
        if (registerWorker(worker)) {
          persistenceEngine.addWorker(worker)
          sender ! RegisteredWorker(loadMasterUrl, loadMasterWebUIUrl)

          registerWorkerStructor(workHost,udpPort)
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
    case BuffersWarn ( workerHost ) =>
      adjustCollectorByBuffer( workerHost,sender)

    case BufferOverFlow ( workerHost ) =>
      adjustCollectorByBuffer( workerHost,sender)

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

    // we should tell all the collectors who connected with this dead worker to connect with living worker
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
  // only used to send the worker info ($2255.255.255.255:65535) to collector
  private lazy val CollectorMessage = {
    //  val preFix = Array('$').asInstanceOf[Array[Byte]]
    val buff = ByteBuffer.allocate(1000)
    buff.put('$'.asInstanceOf[Byte])
    buff.mark()
    buff
  }
  def getCollectorMessage( workers : Array[(String,Int)]) : ByteBuffer = {
    CollectorMessage.reset()  // should not call clear ,since we store the prefix flag "$-" in byte buffer
    CollectorMessage.put(workers.size.asInstanceOf[Byte])   // number
    workers.foreach( x =>{
      CollectorMessage.put( IPv4.str2Bytes(x._1) )
      CollectorMessage.putShort(x._2.asInstanceOf[Short])
    })
    CollectorMessage
  }

  // 通知collector 该修改worker 了
  private def notifyCollector( collectorHost : String, newHost : ( String, Int)) ={
    val availableWorkerList = new ArrayBuffer[ (String,Int) ]()
    availableWorkerList += newHost
    collectorToWorkers.get(collectorHost).get.foreach( x => {
      val port = workerToUdpPort.get(x).get
      availableWorkerList.+=:(x,port)
    })

    collectorToSocket.get(collectorHost).get.write(getCollectorMessage(availableWorkerList.toArray))
  }

  // 这个方法在worker 失效的时候调用 ，需要动态的修改 workerToudpPort, workerToCollector, collectorToworkers
  private def adjustCollectorByDeadworker( deadworker : String): Unit ={
    // 1、获取到这个deadworker上的所有 collector list（a）
    // 2、获取到除了这个deadworker上的所有其他的worker，并将这些worker按照collector list 的大小排序（从小到大）
    // 3、将collector list（a）中的每一个collector 都做下面的事情
    //    1）按次序分给2中的每一个worker的collector list中 （ worker2collectors ）
    //    2）通知这个collector ， worker发生了变化
    //    3) 修改这个修改collector2workers ，删除deadWorker，增加新的worker
    // 4、删除workerToudpPort 中这个deadworker
    workerToUdpPort -= deadworker
    workerToCollectors.remove(deadworker) match {
      case Some( collectors ) =>
        val livingWorkersList =
          workerToCollectors.iterator.toList.sortWith(_._2.size < _._2.size)   // remaining runnable workers

        var idx = 0
        for( i <-0 until collectors.size ){
          val collector = collectors(i)

          val newWorkerhost = livingWorkersList(idx)._1
          val newWorkerPort = workerToUdpPort.get(newWorkerhost).get

          // tell collector
          notifyCollector(collector, (newWorkerhost,newWorkerPort ))

          //change worker2collectors ( add collector to newWorkerhost)
          workerToCollectors.get(newWorkerhost).get += collector

          //change collector2workers ( add newWorkerhost , delete deadworker )
          val newworerlist = collectorToWorkers.get(collector).get.filter(x=> x==deadworker )
          newworerlist += newWorkerhost
          collectorToWorkers += (collector -> newworerlist )

          idx = ( idx + 1 ) % livingWorkersList.size
        }
      case None =>
        logError("The worker %s should be connect with at lest one collector, but now empty"
          .format(deadworker))
    }

  }

  // 这个方法在收到worker 要调节collector的时候调用，主要来减少当前worker连接的collector数目
  // 最后留下的那个collector 必须 和worker 的host 地址一样 （ 如果collector和worker有相同的host的时候）
  private def adjustCollectorByBuffer( worker : String, workerActor : ActorRef ): Unit={
    // 1. 首先找到这个worker（a）相连的所有collector list（a）
    // 2、如果 collector list（a） 的长度 <=1 ，则给 worker（a） 发消息， collector 不再调节
    // 3、如果 collector list（a） 的长度 >1 , 则干下面的事情
    //    1) 看看除了这个worker（a）之外的其他worker上边的collector list（b） 的长度，找到一个最小长度的worker
    //    2）从collector list（a）中找一个 collector list（b) 不包含的collector
    //    3）告诉这个collector新的连接worker（b）
    //    4）将这个collector加入collector list（b)中，并在collector list（a)删除这个collector（b）
    //    5）将collectorToWorkers的值修改（有增加有删除）

    // tell the collectors who connects with this worker to connect another worker
    workerToCollectors.get(worker) match {
      case Some(collectors) =>
        if( collectors.size <= 1)     // we can not adjust the collector
          workerActor ! AdjustThread
        else {
          // 1. get avaliable worker list except current worker, and order then by asc
          // 2. add a collector which is connect with current worker to the first worker in list
          val (newWorkHost,newWorkerCollectors) =
                workerToCollectors.iterator.filter(x=>x._1 == worker)
                                  .toList.sortWith(_._2.size < _._2.size).head

          for( i <- 0 until collectors.size
               if collectors(i) != worker ){    // except the situation than the collector connect with itsself

            val collector = collectors(i)
            if( !newWorkerCollectors.contains(collector) ){

              // tell collector to change the worker
              val newWorkerPort = workerToUdpPort.get(newWorkHost).get
              notifyCollector(collector, (newWorkHost,newWorkerPort ))

              // add the collector to new worker2collectors
              workerToCollectors.get(newWorkHost).get += collector

              //remove the collector from old worker2collectors
              collectors.remove(i)

              // delete old worker and add new worker in collectorToWorkers
              val newWorkerList = collectorToWorkers.get(collector).filter(x=>  x == worker ).get
              newWorkerList += newWorkHost
              collectorToWorkers.put(collector,newWorkerList)
            }
          }
        }

        // should not be take place
      case None => logError(s"the worker $worker is not running? there are no collectors to connect with? ")
    }
  }

  // 这个方法应该在worker注册成功后调用，记录 workerToUdpPort 和 workerToCollectors
  private def registerWorkerStructor(workerHost : String, workerPort:Int ): Unit = {
    workerToUdpPort += ( workerHost->workerPort )
    workerToCollectors += ( workerHost -> new ArrayBuffer[String])
  }

  import akka.actor.SupervisorStrategy._

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(
    maxNrOfRetries = 10,
    withinTimeRange = Duration(2,"s"),
    loggingEnabled = true
    ){
      case _ => Restart
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
      Props(classOf[LoadMaster], host, boundPort, webUiPort, conf), actorName)
    val timeout = AkkaUtils.askTimeout(conf)
    val portsRequest = actor.ask(BoundPortsRequest)(timeout)
    val portsResponse = Await.result(portsRequest, timeout).asInstanceOf[BoundPortsResponse]
    (actorSystem, boundPort, portsResponse.webUIPort)
  }
}
