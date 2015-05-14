package cn.ac.ict.acs.netflow.load2.deploy.LoadDeploy

import java.io.FileNotFoundException
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

import scala.collection.mutable.ArrayBuffer

/**
 * Created by ayscb on 2015/5/12.
 */
trait CollectorService {
  self : LoadMaster =>

  val collectorPort = conf.getInt("netflow.collection.port",10012)
  val collectorService = new Thread ( " collectService ") {
    //    Thread.currentThread().setDaemon(true)
    logInfo(s"[ netflow ] The collect Service is ready to start ")
    private val selector = Selector.open()

    override def run(): Unit = {
      logInfo(s"[ netflow ] The collect Service is running on port $collectorPort ")

      // start service socket
      val serverSocket = java.nio.channels.ServerSocketChannel.open()
      serverSocket.configureBlocking(false)
      serverSocket.bind(new InetSocketAddress(collectorPort))
      serverSocket.register(selector, SelectionKey.OP_ACCEPT)

      while (!Thread.interrupted()) {
        if (selector.select() != 0) {
          val iter = selector.selectedKeys().iterator()
          while (iter.hasNext) {
            val key = iter.next()
            if (key.isAcceptable) {
              addSubConnection(key)
            } else if (key.isReadable) {
              // dealWithReadSubConnection(key)
              logInfo("-----------------ddddd---------")
            } else if (key.isConnectable) {
              logInfo("--------------------------")
            }
            iter.remove()
          }
        }
      }
      selector.close()
      logInfo(s"[ netflow ] The collect Service is closed. ")
    }

    // deal with connection from remote
    private def addSubConnection(key: SelectionKey): Unit = {
      val socketChannel = key.channel().asInstanceOf[ServerSocketChannel].accept() // connect socket
      socketChannel.configureBlocking(false)
      socketChannel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE)
      val remoteIP = socketChannel.getRemoteAddress.asInstanceOf[InetSocketAddress]
        .getAddress.getHostAddress
      registerCollectorStruct(remoteIP, socketChannel)
      logInfo(s"[ netFlow ] The collect Service accepts a connection from $remoteIP. ")
    }

    //    private val requestFlag = Array('#','#','#').asInstanceOf[Array[Byte]]

    private val readTempBuffer = ByteBuffer.allocate(1000)

    // deal with the read connection request
    private def dealWithReadSubConnection(key: SelectionKey): Unit = {
      val channel = key.channel().asInstanceOf[SocketChannel]
      val remoteHost =
        channel.getRemoteAddress.asInstanceOf[InetSocketAddress].getAddress.getHostAddress
      if (!collectorToSocket.contains(remoteHost)) {
        //TODO : lost? when to happened ?
        logError(s"why does not the $remoteHost exist in collectorToSocket ? ")
      } else {
        // TODO : do we need th consider the situation that the connect channel is closed ?
        if (collectorToSocket.get(remoteHost).get.equals(channel)) {
          readTempBuffer.clear()
          channel.read(readTempBuffer)
          readTempBuffer.flip()
          if (readTempBuffer.array().startsWith("###")) {
            assignWorker(remoteHost)
          }
        } else {
          logError(s"why does not the $remoteHost 's channel equal with the value in collectorToSocket ? ")
          collectorToSocket.put(remoteHost, channel)
        }
      }
    }
  }


  //这个方法在collector通过socket首次连接到 master 时调用，维护 collectorToSocket 和 collectorToWorkers
  private def registerCollectorStruct( collectorHost : String , socketChannel: SocketChannel) = {
    if( collectorToSocket.contains(collectorHost) ){
      logError(s"[ netflow ] The collector $collectorHost should not exist in collectorToSocket !")
    }
    if( collectorToWorkers.contains(collectorHost)){
      logError(s"[ netflow ] The collector $collectorHost should not exist in collectorToWorkers !")
    }

    collectorToSocket += ( collectorHost -> socketChannel)
    collectorToWorkers += ( collectorHost -> new ArrayBuffer[String]())
  }

  // 这个方法只在collector首次连接 master时候调用，来为这个collector分配 默认的worker。 其他时候不应该调用
  private def assignWorker( collector : String , workerNum : Int = 1) : Unit = {
    // 1、首先collectorToWorker 中该collector 对应的workerlist应该是空的 ！！！
    // 2、看看当前的worker中有没有个collector的IP地址是一样的，没有的化等2s后再看
    //    1)如果有的话第一个先选这个worker，之后的（workerNum-1）个就在workToCollectors中按次序选择CollectorList最小的
    //    2）如果没有的话，就在workToCollectors中按次序选择CollectorList最小的 workerNum个
    //    3）修改workerToCollectors （ 在这个worker上增加 collector）
    //    4）修改collectorToworkers （ 该collector 上增加该worker ）
    // 3、告诉Collector选择的的worker

    require(collectorToWorkers.get(collector).get.size == 0)

    val workerList = new Array[(String,Int)](workerNum)
    if( !workerToUdpPort.contains(collector)) Thread.sleep(2000)

    var bi = 0
    if( workerToUdpPort.contains(collector) ){
      workerList(bi) = (collector,workerToUdpPort.get(collector).get)
      bi += 1
    }

    val orderByworkerList = workerToCollectors.iterator.toList.sortWith(_._2.size < _._2.size)
    var oi = 0
    for( i <- bi until workerNum ){
      val host = orderByworkerList(oi)._1
      val post = workerToUdpPort.get(host).get
      workerList(i) = (host,post)
      workerToCollectors.get(host).get += collector
      collectorToWorkers.get(collector).get += host
      oi = ( oi + 1) % orderByworkerList.size
    }

    collectorToSocket.get(collector).get.write(getCollectorMessage(workerList))
  }
}

trait CombineService {
  self:LoadWorker =>
  val combineService = new Thread( " combine running "){

    override def run(): Unit = {
      var second = TimeUtil.getPreviousBaseTime(conf,System.currentTimeMillis()/1000)
      val retryMaxNum = 2
      val fs = FileSystem.get(conf.hadoopConfigure)
      for( i <- 0 until retryMaxNum ) {
        val pathStr = TimeUtil.getTimeBasePathBySeconds(conf,second)

        NetFlowCombineMeta.combineFiles(fs,new Path(pathStr),conf) match {
          case parquetState.DIC_NOT_EXIST =>
            second = TimeUtil.getPreviousBaseTime(conf,second)

          case parquetState.NO_DIC =>
            logError("[ Parquet ] The Path %s should be a dictionary.".format(pathStr))
            fs.close()
            return

          case parquetState.DIC_EMPTY =>
            second = TimeUtil.getPreviousBaseTime(conf,second)

          case parquetState.NO_PARQUET =>
            logError("[ Parquet ] The Path %s should be a parquet dictionary.".format(pathStr))
            fs.close()
            return

          case parquetState.UNREADY =>
            Thread.sleep(2000 * ( 1 + 1) )

          case parquetState.FINISH =>
            master ! CombineFinished
            fs.close()
            return

          case parquetState.FAIL =>
            logError("[ Parquet ] weite parquet error .")
            fs.close()
            return
        }
      }
    }
  }
}

trait UDPReceiverService {
  self:LoadWorker =>

  // get the udp thread runner
  private var udpService: Thread = null
  private var udpActualPort: Int = 0

  def startUdpRunner() = {
    val t = Utils.startServiceOnPort(0, doStartUDPRunner, conf, "udp-receiver")
    udpService = t._1
    udpActualPort = t._2
  }

  def stopUdpRunner() =
    if (udpService != null) udpService.interrupt()

  def getActualUDPPort = udpActualPort
  private def doStartUDPRunner(udpPort: Int): (Thread, Int) = {
    val runner =
      new Thread(" Listening udp " + udpPort + " Thread ") {
        override def run(): Unit = {
          val channel = DatagramChannel.open()
          channel.configureBlocking(true)
          channel.socket().bind(new InetSocketAddress(udpPort))
          while (!Thread.interrupted()) {
            val buff = ByteBuffer.allocate(1500)
            channel.receive(buff)
            bufferList.put(buff)
          }
        }
      }
    runner.setDaemon(true)
    (runner, udpPort)
  }
}

trait ResolveNetflowService  {
  self:LoadWorker =>

  // get ResolvingNetflow threads
  private val writerThreadPool = Utils.newDaemonCachedThreadPool("ResolvingNetflow")
  private val writerThreadsQueue = new scala.collection.mutable.Queue[Thread]

  // flag to close all thread ( only worker change this value )
  // if the value is true,
  // all writers will flush the parquet data into disk
  // and write the meta to disk before exit
  private var stopAllWriterThreads = false

  private val ratesQueue = new LinkedBlockingDeque[Double]()
  private var readRateFlag = false

  // the thread to resolve netflow package
  private def netflowWriter = new Thread("resolving Netflow package Thread") {

    private val currentThread = Thread.currentThread()

    private var sampled = false
    // et true after call method 'getCurrentRate'
    private var startTime = 0L
    private var packageCount = 0

    private def getCurrentRate = {
      val rate = 1.0 * (System.currentTimeMillis() - startTime) / packageCount
      startTime = System.currentTimeMillis()
      packageCount = 0
      rate
    }

    writerThreadsQueue.enqueue(currentThread)

    // write data to parquet
    private val netFlowWriter = new AnalysisFlowData(conf)

    override def run(): Unit = {
      while (!Thread.interrupted()) {
        val data = bufferList.take // when list empty , return null
        if (stopAllWriterThreads) {
          netFlowWriter.closeWriter()
          return
        } else if (data != null)
          if (readRateFlag && !sampled) {
            ratesQueue.put(getCurrentRate)
            sampled = true
          } else if (!readRateFlag) {
            sampled = false
          }

        packageCount += 1
        netFlowWriter.analysisnetflow(data)
      }
      netFlowWriter.closeWriter()
    }
  }

  def initResolvingNetFlowThreads(threadNum: Int) = {
    for (i <- 0 until threadNum)
      writerThreadPool.submit(netflowWriter)
  }

  def adjustResolvingNetFlowThreads(newThreadNum: Int) = {
    val currThreadNum = writerThreadsQueue.size

    logInfo(s"current total resolving thread number is $currThreadNum, " +
      s" and will be adjust to $newThreadNum ")

    if (newThreadNum > currThreadNum) {
      // increase threads
      for (i <- 0 until (newThreadNum - currThreadNum)) {
        writerThreadPool.submit(netflowWriter)
      }
    } else {
      // decrease threads
      for (i <- 0 until (currThreadNum - newThreadNum)) {
        writerThreadsQueue.dequeue().interrupt()
      }
    }
  }

  def stopAllResolvingNetFlowThreads() = {
    logInfo(" current threads number is %d, all threads will be stopped".format(writerThreadsQueue.size))
    stopAllWriterThreads = true
    writerThreadsQueue.clear()
    writerThreadPool.shutdown()
  }

  def getCurrentThreadsRate: util.ArrayList[Double] = {
    readRateFlag = true
    Thread.sleep(10)
    val currentThreadsNum = writerThreadsQueue.size
    while (ratesQueue.size() != currentThreadsNum) {} // get all threads rates
    val list = new util.ArrayList[Double]()
    ratesQueue.drainTo(list)
    ratesQueue.clear()
    readRateFlag = false
    list
  }

  def getCurrentThreadsNum: Int = writerThreadsQueue.size
}
