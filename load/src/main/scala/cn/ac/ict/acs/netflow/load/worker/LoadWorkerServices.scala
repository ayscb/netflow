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

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{ServerSocketChannel, SelectionKey, Selector, SocketChannel}
import java.util
import java.util.concurrent.LinkedBlockingDeque

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}

import cn.ac.ict.acs.netflow.load.LoadMessages.CombineFinished
import cn.ac.ict.acs.netflow.load.util.{AnalysisFlowData, ParquetState, NetFlowCombineMeta}
import cn.ac.ict.acs.netflow.util.{TimeUtils, ThreadUtils, Utils}

trait WorkerService {
  self: LoadWorker =>

  // get the tcp thread runner
  private var Service: Thread = null
  private var ActualPort: Int = _
  private var buffcount = 0

  private val receiverToWorker = new mutable.HashMap[String, SocketChannel]()

  def getWorkerServicePort = ActualPort

  def startWorkerService() = {
    val t = Utils.startServiceOnPort(0, doStartRunner, conf, "Receiver-worker")
    Service = t._1
    ActualPort = t._2
  }

  private def doStartRunner(port: Int): (Thread, Int) = {

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
          if(actualPort == 0){
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
                if (key.isAcceptable){
                  acceptConnection(key)
		}else if (key.isReadable){
                  readConnection(key)
		}
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
          logInfo(s"[Netflow] The receiver $remoteHost is connect to master.")

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
    while(actualPort == 0)
      Thread.sleep(1000)
    (thread, actualPort)
  }
}

trait CombineService {
  self: LoadWorker =>
  val combineService = new Thread("Combine Thread ") {

    override def run(): Unit = {
      logInfo(s"[Netfow-Combine] Combine thread is start to running...")
      var second = TimeUtils.getPreviousBaseTime(conf, System.currentTimeMillis() / 1000)
      val retryMaxNum = 2
      val fs = FileSystem.get(new Configuration())
      for (i <- 0 until retryMaxNum) {
        val pathStr = TimeUtils.getTimeBasePathBySeconds(conf, second)

        NetFlowCombineMeta.combineFiles(fs, new Path(pathStr), conf) match {
          case ParquetState.DIC_NOT_EXIST =>
            second = TimeUtils.getPreviousBaseTime(conf, second)

          case ParquetState.NO_DIC =>
            logError("[Netfow-Combine] The Path %s should be a dictionary.".format(pathStr))
            fs.close()
            return

          case ParquetState.DIC_EMPTY =>
            second = TimeUtils.getPreviousBaseTime(conf, second)

          case ParquetState.NO_PARQUET =>
            logError("[Netfow-Combine] The Path %s should be a parquet dictionary.".format(pathStr))
            fs.close()
            return

          case ParquetState.UNREADY =>
            Thread.sleep(2000 * (1 + 1))

          case ParquetState.FINISH =>
            master ! CombineFinished
            fs.close()
            return

          case ParquetState.FAIL =>
            logError("[Netfow-Combine] Write parquet error .")
            fs.close()
            return
        }
      }
    }
  }
}

trait WriteParquetService {
  self: LoadWorker =>

  // get ResolvingNetflow threads
  private val writerThreadPool = ThreadUtils.newDaemonCachedThreadPool("ResolvingNetflow")
  private val writerThreadsQueue = new scala.collection.mutable.Queue[Thread]

  private val ratesQueue = new LinkedBlockingDeque[Double]()
  private var readRateFlag = false

  // the thread to resolve netflow package
  private def netflowWriter =
    new Runnable() {

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
            } else if (!readRateFlag & sampled) {
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
    logInfo((" current threads number is %d, all " +
      "threads will be stopped").format(writerThreadsQueue.size))
    for (i <- 0 until writerThreadsQueue.size)
      writerThreadsQueue.dequeue().interrupt()
    writerThreadPool.shutdown()
  }

  def getCurrentThreadsRate: util.ArrayList[Double] = {
    readRateFlag = true
    val currentThreadsNum = writerThreadsQueue.size
    while (ratesQueue.size() != currentThreadsNum) { Thread.sleep(1) } // get all threads rates
    val list = new util.ArrayList[Double]()
    ratesQueue.drainTo(list)
    ratesQueue.clear()
    readRateFlag = false
    list
  }
}
