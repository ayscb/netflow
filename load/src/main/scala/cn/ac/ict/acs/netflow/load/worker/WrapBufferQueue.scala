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
import java.util.concurrent.{ LinkedBlockingQueue, LinkedBlockingDeque }

class WrapBufferQueue(
    val maxQueueNum: Int,
    val warnThreshold: Int,
    val loadBalanceStrategyFunc: () => Unit,
    val sendOverflowMessage: () => Unit) {

  require(0 < warnThreshold && warnThreshold < 80, "The Queue warnThreshold should be in (0,80).")

  private val bufferQueue = new LinkedBlockingQueue[ByteBuffer](maxQueueNum)

  private val adjustThresholdNum =
    (((warnThreshold - 20) * 1.0 / 100) * maxQueueNum).asInstanceOf[Int]
  private val warnThresholdNum =
    ((warnThreshold * 1.0 / 100) * maxQueueNum).asInstanceOf[Int]

  private val halfNum = 0.5 * maxQueueNum
  private var lastSize = 0
  private var hasCall = false

  // get the element from queue , block when the queue is empty
  def take = bufferQueue.take()

  // put the element to queue, block when the queue is full
  def put(byteBuffer: ByteBuffer) = {
    checkThreshold()
    bufferQueue.put(byteBuffer)
  }

  def size = bufferQueue.size()

  def currUsageRate(): Double = 1.0 * bufferQueue.size() / maxQueueNum

  private def checkThreshold(): Unit = {
    if (lastSize > bufferQueue.size()) {
      lastSize = bufferQueue.size()
      hasCall = false
      return
    } else {
      lastSize = bufferQueue.size()
    }

    if(!hasCall && bufferQueue.size() > warnThresholdNum){
      sendOverflowMessage() // will block.....
      hasCall = true
      return
    }

    if(!hasCall && bufferQueue.size() > adjustThresholdNum){
      loadBalanceStrategyFunc()
      hasCall = true
      return
    }
  }
}
