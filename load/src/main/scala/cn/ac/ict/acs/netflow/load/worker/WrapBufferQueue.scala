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

import java.nio.ByteBuffer
import java.util.concurrent.{LinkedBlockingQueue, LinkedBlockingDeque}

class WrapBufferQueue(
    val maxQueueNum: Int,
    val warnThreshold: Int,
    val loadBalanceStrategyFunc: () => Unit,
    val sendOverflowMessage: () => Unit) {

  require(0 < warnThreshold && warnThreshold < 100,
    message = " The Queue warnThreshold should be in (0,100) ")

  private val bufferQueue = new LinkedBlockingQueue[ByteBuffer](maxQueueNum)
 // private val bufferQueue = new SynchronousQueue[ByteBuffer]()
  private val warnThresholdNum = ((warnThreshold * 0.1 / 100 ) * maxQueueNum ).asInstanceOf[Int]

  // get the element from queue , block when the queue is empty
  def take = {
    println("\t\t\t\ttake data :" + bufferQueue.size() +" ---- " + bufferQueue.remainingCapacity())
    bufferQueue.take()
  }

  // put the element to queue, block when the queue is full
  def put(byteBuffer: ByteBuffer) = {
    checkThreshold()
    bufferQueue.put(byteBuffer)
  //  println("put into data :" + bufferQueue.size() +" ---- " + bufferQueue.remainingCapacity())
  }

  // get the element from queue , return null when the queue is empty
  def poll = { bufferQueue.poll() }

  // return false when the queue is full
  def offer(byteBuffer: ByteBuffer) = {
    checkThreshold()
    bufferQueue.offer(byteBuffer)
  }

  def getcurrentBufferRate() : Int = {
    // 10*(maxQueueNum - bufferQueue.remainingCapacity())/maxQueueNum
    10*bufferQueue.size()/maxQueueNum
  }
  private def checkThreshold() = {
    if(bufferQueue.size() > warnThresholdNum ){    // warn
      loadBalanceStrategyFunc
    } else if (bufferQueue.remainingCapacity() < 10) {
      // will block.....
      sendOverflowMessage
    }
//    if(bufferQueue.size() > warnThresholdNum){
//      loadBalanceStrategyFunc
//    }else if(bufferQueue.size() > maxQueueNum - 10){
//      sendOverflowMessage
//    }
  }

//  def setWarnThreshold(newWarnThreshold: Int) = {
//    if (0 < newWarnThreshold && newWarnThreshold < 100) {
//      warnThresholdNum = maxQueueNum * newWarnThreshold / 100
//    } else {
//      throw new IllegalArgumentException(" newWarnThreshold should be in (0,100) ")
//    }
//  }
}
