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
import java.util.concurrent.{ TimeUnit, Executors, LinkedBlockingQueue, LinkedBlockingDeque }

import cn.ac.ict.acs.netflow.load.LoadConf
import cn.ac.ict.acs.netflow.{ NetFlowConf, Logging }
import cn.ac.ict.acs.netflow.util.ThreadUtils

class WrapBufferQueue(
    val loadBalanceStrategyFunc: () => Unit,
    val sendOverflowMessage: () => Unit,
    val conf: NetFlowConf) extends Logging {

  private val maxQueueNum = conf.getInt(LoadConf.QUEUE_MAXPACKAGE_NUM, 1000000)
  private val warnThreshold = {
    val threshold = conf.getInt(LoadConf.QUEUE_WARN_THRESHOLD, 70)
    if (0 < threshold && threshold < 100) threshold else 70
  }
  private val reportMasterDelay = conf.getInt(LoadConf.REPORT_MASTER_DELAY, 5 * 1000)
  private val reportWorkerDelay = conf.getInt(LoadConf.REPORT_WORKER_DELAY, 3 * 1000)

  private val adjustThresholdNum =
    (((warnThreshold - 20) * 1.0 / 100) * maxQueueNum).asInstanceOf[Int]
  private val warnThresholdNum = ((warnThreshold * 1.0 / 100) * maxQueueNum).asInstanceOf[Int]

  private val bufferQueue = new LinkedBlockingQueue[ByteBuffer](maxQueueNum)


  @volatile private var reportMasterFlag: Boolean = false
  @volatile private var reportWorkerFlag: Boolean = false
  @volatile var lastQueueSize: Int = 0
  private val scheduledThreadPool = Executors.newScheduledThreadPool(2)

  private def runnable(delay: Long, isMasterReport: Boolean) = new Runnable {
    override def run(): Unit = {
      val curSize = bufferQueue.size()
      val curRate = 1.0 * (curSize - lastQueueSize) / delay
      if (curRate > 0) {
        // means the rate is increase
        if (isMasterReport) {
          reportMasterFlag = true
        } else {
          reportWorkerFlag = true
        }
      }
      lastQueueSize = curSize
    }
  }

  scheduledThreadPool.scheduleWithFixedDelay(
    runnable(reportMasterDelay, isMasterReport = true),
    0, reportMasterDelay, TimeUnit.MILLISECONDS)

  scheduledThreadPool.scheduleWithFixedDelay(
    runnable(reportWorkerDelay, isMasterReport = false),
    0, reportWorkerDelay, TimeUnit.MILLISECONDS)

  // get the element from queue , block when the queue is empty
  def take = bufferQueue.take()

  // put the element to queue, block when the queue is full
  def put(byteBuffer: ByteBuffer) = {
    checkThreshold()
    bufferQueue.put(byteBuffer)
  }

  def currSize = bufferQueue.size()
  def bufferCapability = maxQueueNum
  def currUsageRate(): Double = 1.0 * bufferQueue.size() / maxQueueNum

  private def checkThreshold(): Unit = {

    val curSize = bufferQueue.size()
    if (reportMasterFlag && curSize > warnThresholdNum) {
      sendOverflowMessage() // will block.....
      reportMasterFlag = false
      return
    }

    if (reportWorkerFlag && curSize > adjustThresholdNum) {
      loadBalanceStrategyFunc()
      reportWorkerFlag = false
      return
    }
  }
}
