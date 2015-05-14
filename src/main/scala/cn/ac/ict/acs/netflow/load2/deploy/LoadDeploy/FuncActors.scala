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
import java.nio.channels.{SelectionKey, Selector, DatagramChannel}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{TimeUnit, ArrayBlockingQueue}
import akka.actor.{ActorRef, Actor}
import cn.ac.ict.acs.netflow.load2.deploy.LoadWorkerMessage._
import cn.ac.ict.acs.netflow.load2.netFlow.AnalysisFlowData
import cn.ac.ict.acs.netflow.{NetFlowConf, Logging}
import cn.ac.ict.acs.netflow.util.ActorLogReceive
import scala.actors.threadpool.Executors
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

import scala.concurrent.ExecutionContext.Implicits.global
/**
 * Created by ayscb on 2015/5/4.
 */

// UDPActor to worker
case object bufferlistOverflow

class UDPActor(
              bufferList : ArrayBlockingQueue[ByteBuffer],
              worker: ActorRef
              )
  extends Actor with ActorLogReceive with Logging {

  val udpPort = NetUtil.getAvailablePort

  lazy val udpSelectorRunner = new Thread("Listening Selector udp Thread") {
    override def run(): Unit = {
      // init the udp client
      val dc = DatagramChannel.open()
      dc.configureBlocking(false)
      dc.socket().bind(new InetSocketAddress(udpPort))

      val selector = Selector.open()
      dc.register(selector, SelectionKey.OP_READ)
      while (!Thread.interrupted()) {
        if (selector.select(100) != 0) {
          val keyIter = selector.selectedKeys().iterator()
          while (keyIter.hasNext) {
            val key = keyIter.next()

            if( key.isReadable ){
              val channel = key.channel().asInstanceOf[DatagramChannel]
              val buff = ByteBuffer.allocate(1500)
              channel.receive(buff)
              if ( !bufferList.offer(buff) ){
                worker ! bufferlistOverflow
              }
            }
            keyIter.remove()
          }
        }
      }
    }
}

  lazy val udpSingleRunner = new Thread( " Listening udp " + udpPort + " Thread ") {
    override def run(): Unit = {
      val channel = DatagramChannel.open()
      channel.configureBlocking(true)
      channel.socket().bind(new InetSocketAddress(udpPort))
      while (!Thread.interrupted()) {
        val buff = ByteBuffer.allocate(1500)
        channel.receive(buff)
        if( !bufferList.offer(buff) )
          worker ! bufferlistOverflow
      }
    }
  }

  override def preStart(): Unit = {
    logInfo(s"preStart Listening upd package on port $udpPort. ")
  }

  override def receiveWithLogging: Receive = {
    case UDPReceiveStart =>
      logInfo(s"start Listening udp thread .....")
      udpSingleRunner.start()

    case UDPReceiveStop =>
      logInfo(s"stop Listening udp thread .....")
      udpSingleRunner.interrupt()
  }
}


class ResolvingNetflow(
                     bufferList : ArrayBlockingQueue[ByteBuffer],
                     conf : NetFlowConf)
  extends Actor with ActorLogReceive with Logging {

  private val threadPool = Executors.newCachedThreadPool()
  // current threads list
  private val threadsStack = new mutable.Stack[Thread]

  // the flag to close all threads
  private val stopAll = new AtomicBoolean()
  stopAll.set(false)

  // the thread to resolve netflow package
  private def resolvingNetflow = new Thread("resolving Netflow package Thread"){

    threadsStack.push(Thread.currentThread())
    val resolvingObj = new AnalysisFlowData(conf)

    override def run(): Unit = {
      while(!Thread.interrupted()){
          val data = bufferList.peek()    // when list empty , return null
          if( stopAll.get && data == null ){
            resolvingObj.closeWriter()
            return
          }else if( data != null )
            resolvingObj.analysisnetflow(data)
          }
      resolvingObj.closeWriter()
    }
  }

  private def resetThreads( threadNum : Int) = {
    val currThreadNum = threadsStack.size

    logInfo( s"current total resolving thread number is $currThreadNum, " +
      s" and will be adjust to $threadNum ")

    if( threadNum > currThreadNum ){
      // increase threads
      for( i <- 0 until ( threadNum - currThreadNum)){
        threadPool.submit(resolvingNetflow)
      }
    }else{
      // decrease threads
      for( i <- 0 until (currThreadNum - threadNum)){
        threadsStack.pop()interrupt()
      }
    }
  }

  private def closeAllThreads = {
    logInfo(" current threads number is %d, all threads will be stopped".format(threadsStack.size))
    stopAll.set(true)
    threadsStack.clear()
    threadPool.shutdown()
  }

  override def receiveWithLogging: Actor.Receive = {
    case LaunchResolvingThread( threadNum ) => resetThreads( threadNum )
    case AdjustResolvingThread( threadNum ) => resetThreads( threadNum )
    case CloseAllResolvingThread => closeAllThreads
  }
}

// Monitor to Monitor
case object checkBuffer

// worker to Monitor
case object MonitorClose
case class changeWarnThreshold( newThreshold : Int )

// Monitor to worker
case object BufferListWarn

class MonitoringBuffer (
                         bufferList : ArrayBlockingQueue[ByteBuffer],
                         worker: ActorRef,
                         Threshold : Int )
  extends Actor with ActorLogReceive with Logging {

  private var warnThreshold = Threshold

  override def preStart(): Unit = {
    context.system.scheduler.schedule(
      FiniteDuration(0,TimeUnit.SECONDS),
      FiniteDuration(1,TimeUnit.SECONDS),
      self,checkBuffer
    )
  }

  override def receiveWithLogging: Actor.Receive = {
    case checkBuffer =>
      if( bufferList.remainingCapacity() < warnThreshold )
        worker ! BufferListWarn

    case MonitorClose =>
      context.system.shutdown()

    case changeWarnThreshold( newThreshold ) =>
      warnThreshold = newThreshold
  }
}