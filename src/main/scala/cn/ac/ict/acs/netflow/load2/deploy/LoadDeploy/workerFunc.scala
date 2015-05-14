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
import java.util
import java.util.concurrent.{LinkedBlockingDeque, ArrayBlockingQueue}
import cn.ac.ict.acs.netflow.{Logging, NetFlowConf}
import cn.ac.ict.acs.netflow.load2.netFlow.AnalysisFlowData
import cn.ac.ict.acs.netflow.util.Utils


/**
 * Created by ayscb on 2015/5/6.
 */
class WrapBufferQueue(
                     maxQueueNum : Int,
                     warnThreshold : Int,
                     loadBalanceStrategyFunc : ()=>Unit,
                     sendOverflowMessage : ()=>Unit
                     ){

  require (0 < warnThreshold && warnThreshold < 100 ,
    message = " The Queue warnThreshold should be in (0,100) ")

  private val bufferQueue = new ArrayBlockingQueue[ByteBuffer](maxQueueNum)
  private var warnThresholdNum = maxQueueNum * warnThreshold / 100

  // get the element from queue , block when the queue is empty
  def take = synchronized{
    bufferQueue.take()
  }

  // put the element to queue, block when the queue is full
  def put( byteBuffer : ByteBuffer) = {
    checkThreshold()
    bufferQueue.put(byteBuffer)
  }

  private def checkThreshold() = {
    if( bufferQueue.remainingCapacity() < warnThresholdNum )    // warn
      loadBalanceStrategyFunc
    else if ( bufferQueue.remainingCapacity() < 10 )            // will block.....
      sendOverflowMessage
  }

  def setWarnThreshold( newWarnThreshold : Int ) = {
    if( 0 < newWarnThreshold && newWarnThreshold < 100 )
      warnThresholdNum = maxQueueNum * newWarnThreshold / 100
    else
      throw new IllegalArgumentException(" newWarnThreshold should be in (0,100) ")
  }
}

//**********************************************************
//
//class UDPService(
//                  bufferList : WrapBufferQueue ,
//                  conf : NetFlowConf )
//    extends Logging {
//
//  // get the udp thread runner
//  var udpService: Thread = null
//  var udpActualPort: Int = 0
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
//
//}
// //************************************ ResolvingNetflow ************************************
//
//class ResolveNetflow(
//                      bufferList : WrapBufferQueue ,
//                      conf : NetFlowConf )
//extends Logging{
//
//  // get ResolvingNetflow threads
//  private val writerThreadPool = Utils.newDaemonCachedThreadPool("ResolvingNetflow")
//  private val writerThreadsQueue = new scala.collection.mutable.Queue[Thread]
//
//  // flag to close all thread ( only worker change this value )
//  // if the value is true,
//  // all writers will flush the parquet data into disk
//  // and write the meta to disk before exit
//  private var stopAllWriterThreads = false
//
//  private val ratesQueue = new LinkedBlockingDeque[Double]()
//  private var readRateFlag = false
//
//  // the thread to resolve netflow package
//  private def netflowWriter = new Thread("resolving Netflow package Thread"){
//
//    private val currentThread = Thread.currentThread()
//
//    private var sampled = false   // et true after call method 'getCurrentRate'
//    private var startTime = 0L
//    private var packageCount = 0
//    private def getCurrentRate = {
//      val rate = 1.0 * ( System.currentTimeMillis() - startTime ) / packageCount
//      startTime = System.currentTimeMillis()
//      packageCount = 0
//      rate
//    }
//
//    writerThreadsQueue.enqueue(currentThread)
//
//    // write data to parquet
//    private val netFlowWriter = new AnalysisFlowData(conf)
//
//    override def run(): Unit = {
//      while(!Thread.interrupted()){
//        val data = bufferList.take    // when list empty , return null
//        if( stopAllWriterThreads ){
//          netFlowWriter.closeWriter()
//          return
//        }else if( data != null )
//          if( readRateFlag && !sampled ){
//            ratesQueue.put(getCurrentRate)
//            sampled = true
//          }else if ( !readRateFlag ){
//            sampled = false
//          }
//
//          packageCount += 1
//          netFlowWriter.analysisnetflow(data)
//      }
//      netFlowWriter.closeWriter()
//    }
//  }
//
//  def initResolvingNetFlowThreads( threadNum : Int ) = {
//    for ( i <- 0 until threadNum )
//      writerThreadPool.submit(netflowWriter)
//  }
//
//  def adjustResolvingNetFlowThreads( newThreadNum : Int ) = {
//    val currThreadNum = writerThreadsQueue.size
//
//    logInfo( s"current total resolving thread number is $currThreadNum, " +
//      s" and will be adjust to $newThreadNum ")
//
//    if( newThreadNum > currThreadNum ){
//      // increase threads
//      for( i <- 0 until ( newThreadNum - currThreadNum)){
//        writerThreadPool.submit(netflowWriter)
//      }
//    }else{
//      // decrease threads
//      for( i <- 0 until (currThreadNum - newThreadNum)){
//        writerThreadsQueue.dequeue().interrupt()
//      }
//  }
//}
//
//  def stopAllResolvingNetFlowThreads() = {
//    logInfo(" current threads number is %d, all threads will be stopped".format(writerThreadsQueue.size))
//    stopAllWriterThreads = true
//    writerThreadsQueue.clear()
//    writerThreadPool.shutdown()
//  }
//
//  def getCurrentThreadsRate : util.ArrayList[Double] = {
//    readRateFlag = true
//    Thread.sleep(10)
//    val currentThreadsNum = writerThreadsQueue.size
//    while( ratesQueue.size() != currentThreadsNum ){}   // get all threads rates
//    val list = new util.ArrayList[Double]()
//    ratesQueue.drainTo(list)
//    ratesQueue.clear()
//    readRateFlag = false
//    list
//  }
//
//  def getCurrentThreadsNum : Int = writerThreadsQueue.size
//}