package cn.ac.ict.acs.netflow.load2.deploy.loadDeploy

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels._
import java.util
import java.util.concurrent.LinkedBlockingDeque
import cn.ac.ict.acs.netflow.TimeUtil
import cn.ac.ict.acs.netflow.load2.deploy.LoadWorkerMessage.CombineFinished
import cn.ac.ict.acs.netflow.load2.netFlow.AnalysisFlowData
import cn.ac.ict.acs.netflow.load2.parquetUtil.{parquetState, NetFlowCombineMeta}
import cn.ac.ict.acs.netflow.util.Utils
import org.apache.hadoop.fs.{Path, FileSystem}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Created by ayscb on 2015/5/12.
 */
object Mode extends Enumeration {
  type Mode = Value
  val add, delete = Value
}

object netUtil {
  import Mode._

  private val buffer = ByteBuffer.allocate(1500)

  /**
   *
   * @param mode
   * + add new ips
   * - delete exist ips
   * @param ipAdds
   * @return
   * if the ipAdds is available, return ips
   * else return "null"
   */
  def responseReceiver(mode: Mode, ipAdds: Array[(String, Int)]): ByteBuffer = {
    buffer.clear()
    if( mode == add ){
      val str = "$$+" +
        ipAdds.length.toString + "&" +
        ipAdds.map(ip => ip._1.concat(":" + ip._2)).mkString("&")
      buffer.put(str.getBytes)
    }else if( mode == delete ){
      val str = "$$-" +
        ipAdds.length.toString + "&" +
        ipAdds.map(ip => ip._1.concat(":" + ip._2)).mkString("&")
      buffer.put(str.getBytes)
    }
    buffer.flip()
    buffer
  }

  def getBuffer: ByteBuffer = {
    buffer.clear()
    buffer
  }
}

// run in worker
trait CombineService {
  self: LoadWorker =>
  val combineService = new Thread("Combine Thread ") {

    override def run(): Unit = {
      logInfo(s"[Netfow-Combine] Combine thread is start to running...")
      var second = TimeUtil.getPreviousBaseTime(conf, System.currentTimeMillis() / 1000)
      val retryMaxNum = 2
      val fs = FileSystem.get(conf.hadoopConfigure)
      for (i <- 0 until retryMaxNum) {
        val pathStr = TimeUtil.getTimeBasePathBySeconds(conf, second)

        NetFlowCombineMeta.combineFiles(fs, new Path(pathStr), conf) match {
          case parquetState.DIC_NOT_EXIST =>
            second = TimeUtil.getPreviousBaseTime(conf, second)

          case parquetState.NO_DIC =>
            logError("[Netfow-Combine] The Path %s should be a dictionary.".format(pathStr))
            fs.close()
            return

          case parquetState.DIC_EMPTY =>
            second = TimeUtil.getPreviousBaseTime(conf, second)

          case parquetState.NO_PARQUET =>
            logError("[Netfow-Combine] The Path %s should be a parquet dictionary.".format(pathStr))
            fs.close()
            return

          case parquetState.UNREADY =>
            Thread.sleep(2000 * (1 + 1))

          case parquetState.FINISH =>
            master ! CombineFinished
            fs.close()
            return

          case parquetState.FAIL =>
            logError("[Netfow-Combine] Write parquet error .")
            fs.close()
            return
        }
      }
    }
  }
}

// run in master
trait MasterService {
  self: LoadMaster =>

  // get the tcp thread runner
  private var Service: Thread = null
  private var ActualPort: Int = 0

  def StopThread() = if (Service != null) Service.interrupt()
  def getActualPort = ActualPort

  def startMasterService() = {
    val receiverPort = conf.getInt("netflow.receiver.port", 10012)
    val t = Utils.startServiceOnPort(receiverPort, doStartRunner, conf, "Receiver-Master")
    Service = t._1
    ActualPort = t._2
  }

  private def doStartRunner(Port: Int): (Thread, Int) = {
    val thread =
      new Thread("Receiver-Master-Service") {

        logInfo(s"[Netflow] The Service for Receiver is ready to start ")
        private val selector = Selector.open()

        override def run(): Unit = {
          logInfo(s"[Netflow] The Service for Receiver is running on port $Port ")

          // start service socket
          val serverSocket = java.nio.channels.ServerSocketChannel.open()
          serverSocket.configureBlocking(false)
          serverSocket.socket().bind(new InetSocketAddress(Port))
          serverSocket.register(selector, SelectionKey.OP_ACCEPT)

          while (!Thread.interrupted()) {
            if (selector.select() != 0) {
              val iter = selector.selectedKeys().iterator()
              while (iter.hasNext) {
                val key = iter.next()
                iter.remove()
                if (key.isAcceptable) {
                  acceptConnection(key)
                } else if (key.isReadable) {
                  readConnection(key)
                } else if (key.isWritable) {
                 // writeConnection(key)
                }else if( key.isValid){
                  printf("valid")
                }else if(key.isConnectable){
                  printf("isConnectable")
                }
              }
            }
          }
          selector.close()
          logInfo(s"[Netflow] The Service for Receiver is closed. ")
        }

        /* deal with connection from remote */
        private def acceptConnection(key: SelectionKey): Unit = {
          // connect socket
          val socketChannel = key.channel().asInstanceOf[ServerSocketChannel].accept()
          socketChannel.configureBlocking(false)
          socketChannel.register(selector, SelectionKey.OP_READ)

          // save the remote ip
          val remoteIP = socketChannel.getRemoteAddress.asInstanceOf[InetSocketAddress]
            .getAddress.getHostAddress
          registerReceiverStruct(remoteIP, socketChannel)

          logInfo(s"[Netflow] The receiver Service accepts a connection from $remoteIP.receiver ")
        }

        // deal with the read connection request
        private def readConnection(key: SelectionKey): Unit = {
          val channel = key.channel().asInstanceOf[SocketChannel]
          val remoteHost =
            channel.getRemoteAddress.asInstanceOf[InetSocketAddress]
              .getAddress.getHostAddress

          logInfo(s"[Netflow] The $remoteHost receiver is request to master.")

          val buff = netUtil.getBuffer
          val count = channel.read(buff)
          if (count > 0) {
            buff.flip()
            if (buff.array.startsWith("$req$")){
              val prefix_len = "$req$".length
              if(buff.limit() != prefix_len && buff.get(prefix_len) == '-'){
                // get dead ip
                buff.position(prefix_len+1)
                val ips = new Array[Byte](40)
                val len = Math.min(40,buff.remaining())
                buff.get(ips, 0, len)
                val ipStr = new String(ips,0,len).split(":")
                if( workerToPort.contains(ipStr(0))){
                  if( workerToPort(ipStr(0))._2 == ipStr(1).toInt){
                    workerToPort -= ipStr(0)
                    workerToBufferRate -= ipStr(0)
                    workerToReceivers -= ipStr(0)
                    receiverToWorkers(remoteHost)-= ipStr(0)
                  }
                }
              }

              // get worker ips
              val data = assignWorker(remoteHost)
              if (data != None) {
                  channel.write(netUtil.responseReceiver(Mode.add, data.get))
              }else{
                  // No available worker to assigned
                  waitQueue += (remoteHost -> channel )
              }
            }

            channel.register(selector, SelectionKey.OP_READ)
          }else if(count == -1){
            // close socket
            channel.shutdownInput()
            receiverToSocket -= remoteHost
            receiverToWorkers -= remoteHost
            workerToReceivers.foreach(x=>{
              x._2 -= remoteHost
            })
            key.cancel()
            logInfo(s"[Netflow] The $remoteHost receiver is closed")
          }
        }

        // the method is called only by first connection for receiverIP
        private def registerReceiverStruct(receiverIP: String, socketChannel: SocketChannel) = {
          if (receiverToSocket.contains(receiverIP)) {
            logError(s"[Netflow] The receiver $receiverIP should not exist in receiverToSocket !")
          }
          if (receiverToWorkers.contains(receiverIP)) {
            logError(s"[Netflow] The receiver $receiverIP should not exist in receiverToWorkers !")
          }

          receiverToSocket += (receiverIP -> socketChannel)
          receiverToWorkers += (receiverIP -> new ArrayBuffer[String]())
        }

        // assign worker
        private def assignWorker(receiver: String, workerNum: Int = 1): Option[Array[(String, Int)]] = {
          receiverToWorkers.get(receiver) match {
            case None =>
              logError(s"[ Netflow ] The node $receiver should be registed! ")
              None
            case Some(workers) =>
              if (workers.size == 0) {
                // the first time to assign worker

                if( workerToPort.size == 0 ){
                  logWarning(s"[Netflow] There is no available worker to run in cluster.")
                  return None
                }

                // first, try to assign itself
                if (!workerToPort.contains(receiver)) Thread.sleep(2000)
                if (workerToPort.contains(receiver)) {
                  workers += receiver
                }

                // if needs more than one worker
                if (workers.size != workerNum) {

                  val orderByworkerList = workerToBufferRate.filterNot(x=>x._1 == receiver)
                    .toList.sortWith(_._2 < _._2)

                  var oi = 0
                  while (workers.size != workerNum) {
                    val host = orderByworkerList(oi)._1
                    workers += host
                    oi = (oi + 1) % orderByworkerList.size
                  }
                }
              }

              val workerList = new Array[(String, Int)](workerNum)
              for (i <- 0 until workers.size) {
                workerList(i) = workerToPort.get(workers(i)).get
              }
              Some(workerList)
          }
        }
      }

    thread.start()
    (thread, Port)
  }
}

// run in worker
trait WorkerService{
  self: LoadWorker =>

  // get the tcp thread runner
  private var Service: Thread = null
  private var ActualPort: Int = _
  private var buffcount = 0

  private val receiverToWorker = new mutable.HashMap[String,SocketChannel]()

  def getWorkerServicePort = ActualPort

  def startWorkerService() = {
    val t =Utils.startServiceOnPort(0, doStartRunner, conf, "Receiver-worker")
    Service = t._1
    ActualPort = t._2
  }

  private def doStartRunner(port: Int): (Thread, Int) ={

    var actualPort = port

    val thread =
      new Thread("Receiver-Worker-Service") {
        logInfo(s"[Netflow] The Service for Receiver is ready to start ")
        private val selector = Selector.open()

        override def run(): Unit = {

          // start service socket
          val serverSocketChannel = java.nio.channels.ServerSocketChannel.open()
          serverSocketChannel.configureBlocking(false)
          val serverSocket = serverSocketChannel.socket()
          if( actualPort == 0 ){
            serverSocket.bind(null)
            actualPort = serverSocket.getLocalPort
          }else{
            serverSocket.bind(new InetSocketAddress(actualPort))
          }

          logInfo(s"[Netflow] The Service for Receiver is running on port $actualPort ")

          serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT)

          while (!Thread.interrupted()) {
            if (selector.select() != 0) {
              val iter = selector.selectedKeys().iterator()
              while (iter.hasNext) {
                val key = iter.next()
                if (key.isAcceptable)
                  acceptConnection(key)
                else if (key.isReadable)
                  readConnection(key)
                iter.remove()
              }
            }
          }
          selector.close()
          logInfo(s"[Netflow] The Service for Receiver is closed. ")
        }

        /* deal with connection from remote */
        private def acceptConnection(key: SelectionKey): Unit = {
          // connect socket
          val socketChannel = key.channel().asInstanceOf[ServerSocketChannel].accept()
          socketChannel.configureBlocking(false)
          socketChannel.register(selector, SelectionKey.OP_READ)

          // save the remote ip
          val remoteIP = socketChannel.getRemoteAddress.asInstanceOf[InetSocketAddress]
            .getAddress.getHostAddress
          receiverToWorker += (remoteIP -> socketChannel)
          logInfo(s"[Netflow] The receiver Service accepts a connection from $remoteIP. ")
        }

        private def readConnection(key: SelectionKey): Unit = {
          val channel = key.channel().asInstanceOf[SocketChannel]
          val remoteHost =
            channel.getRemoteAddress.asInstanceOf[InetSocketAddress]
              .getAddress.getHostAddress

          val buff = ByteBuffer.allocate(1500)
          val count = channel.read(buff)
          if (count > 0) {
            buff.flip()
            bufferList.put(buff)
          //  bufferList.offer(buff)
            buffcount += 1

            channel.register(selector, SelectionKey.OP_READ)
          }else if(count == -1){
            // close socket
            channel.shutdownInput()
            key.cancel()
            logInfo(s"[Netflow] The $remoteHost receiver is closed")
          }
        }
      }

    thread.start()
    while( actualPort == 0)
      Thread.sleep(1000)
    (thread, actualPort)
  }
}

//
//@Deprecated
//trait UDPReceiverService {
//  self: LoadWorker =>
//
//  // get the udp thread runner
//  private var udpService: Thread = null
//  private var udpActualPort: Int = 0
//
//  def startUdpRunner() = {
//    val t = Utils.startServiceOnPort(0, doStartUDPRunner, conf, "udp-receiver")
//    udpService = t._1
//    udpActualPort = t._2
//  }
//
//  def stopUdpRunner() =
//    if (udpService != null) udpService.interrupt()
//
//  def getActualUDPPort = udpActualPort
//
//  private def doStartUDPRunner(udpPort: Int): (Thread, Int) = {
//    val runner =
//      new Thread(" Listening udp " + udpPort + " Thread ") {
//        override def run(): Unit = {
//          val channel = DatagramChannel.open()
//          channel.configureBlocking(true)
//          channel.socket().bind(new InetSocketAddress(udpPort))
//          while (!Thread.interrupted()) {
//            val buff = ByteBuffer.allocate(1500)
//            channel.receive(buff)
//            bufferList.put(buff)
//          }
//        }
//      }
//    runner.setDaemon(true)
//    (runner, udpPort)
//  }
//}

// run in worker
trait WriteParquetService {
  self: LoadWorker =>

  // get ResolvingNetflow threads
  private val writerThreadPool = Utils.newDaemonCachedThreadPool("ResolvingNetflow")
  private val writerThreadsQueue = new scala.collection.mutable.Queue[Thread]

  private val ratesQueue = new LinkedBlockingDeque[Double]()
  private var readRateFlag = false

  // the thread to resolve netflow package
  private def netflowWriter =
    new Runnable(){

      private var sampled = false
      // et true after call method 'getCurrentRate'
      private var startTime = 0L
      private var packageCount = 0

      private def getCurrentRate = {
        val rate = 1.0 * packageCount / (System.currentTimeMillis() - startTime)
        startTime = System.currentTimeMillis()
        packageCount = 0
        rate
      }

      // write data to parquet
      private val netFlowWriter = new AnalysisFlowData(conf)

      override def run() : Unit = {
        logInfo("[Netflow] Start sub Write Parquet %d".format(Thread.currentThread().getId) )
        writerThreadsQueue.enqueue(Thread.currentThread())
        while (!Thread.interrupted()) {
        //  val data = bufferList.poll // when list empty , null
          val data = bufferList.take
          if(data != null){
            if (readRateFlag && !sampled) {
              ratesQueue.put(getCurrentRate)
              sampled = true
            }else if (!readRateFlag & sampled) {
              sampled = false
            }
            packageCount += 1
            netFlowWriter.analysisnetflow(data)
          }
        }
        netFlowWriter.closeWriter()
      }
  }

  def initParquetWriterPool(threadNum: Int) = {
    for (i <- 0 until threadNum)
      writerThreadPool.submit(netflowWriter)
  }

  def getCurrentThreadsNum: Int = writerThreadsQueue.size

  def adjustResolvingNetFlowThreads(newThreadNum: Int) = {
    val currThreadNum = writerThreadsQueue.size
    logInfo(s"[Netflow] Current total resolving thread number is $currThreadNum, " +
      s" and will be adjust to $newThreadNum ")

    if (newThreadNum > currThreadNum) {
      // increase threads
      for (i <- 0 until (newThreadNum - currThreadNum))
        writerThreadPool.submit(netflowWriter)
    } else {
      // decrease threads
      for (i <- 0 until (currThreadNum - newThreadNum))
        writerThreadsQueue.dequeue().interrupt()
    }
  }

  def stopAllResolvingNetFlowThreads() = {
    logInfo("[Netflow] Current threads number is %d, all threads will be stopped".format(writerThreadsQueue.size))
    for( i <- 0 until writerThreadsQueue.size )
      writerThreadsQueue.dequeue().interrupt()
    writerThreadPool.shutdown()
  }

  def getCurrentThreadsRate: util.ArrayList[Double] = {
    readRateFlag = true
    val currentThreadsNum = writerThreadsQueue.size
    while (ratesQueue.size() != currentThreadsNum) {Thread.sleep(1)} // get all threads rates
    val list = new util.ArrayList[Double]()
    ratesQueue.drainTo(list)
    ratesQueue.clear()
    readRateFlag = false
    list
  }
}
