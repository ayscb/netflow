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

import java.util

import akka.actor.ActorRef
import cn.ac.ict.acs.netflow.load.worker.parser.PacketParser._
import cn.ac.ict.acs.netflow.{ Logging, NetFlowConf }
import cn.ac.ict.acs.netflow.load.worker.parquet.ParquetWriterWrapper
import cn.ac.ict.acs.netflow.util.ThreadUtils
import scala.collection._

final class LoaderService(
    private val workerActor: ActorRef,
    private val bufferList: WrapBufferQueue,
    private val conf: NetFlowConf) extends Logging {

  private val writerThreadPool = ThreadUtils.newDaemonCachedThreadPool("parquetWriterPool")

  private val writeThreadRate = new mutable.HashMap[Thread, Double]()
  private val RateCount = new java.util.concurrent.atomic.AtomicInteger()

  @volatile private var readRateFlag = false

  def initParquetWriterPool(threadNum: Int) = {
    for (i <- 0 until threadNum) {
      writerThreadPool.submit(newWriter)
    }
  }

  def curThreadsNum: Int = writeThreadRate.size

  def adjustWritersNum(newThreadNum: Int) = {

    val _curThreadsNum = curThreadsNum
    logInfo(s"[Netflow] Current total resolving thread number is ${_curThreadsNum}, " +
      s" and will be adjust to $newThreadNum ")

    if (newThreadNum > _curThreadsNum) {
      // increase threads
      for (i <- 0 until (newThreadNum - _curThreadsNum))
        writerThreadPool.submit(newWriter)
    } else {
      writeThreadRate.synchronized({
        writeThreadRate.keySet.take(_curThreadsNum - newThreadNum)
          .foreach(thread => {
            writeThreadRate -= thread
            thread.interrupt()
          })
      })
    }
  }

  def stopAllWriterThreads() = {
    logInfo((" current threads number is %d, all " +
      "threads will be stopped").format(curThreadsNum))
    writeThreadRate.synchronized({
      writeThreadRate.keySet.foreach(_.interrupt())
      writeThreadRate.clear()
    })
    writerThreadPool.shutdown()
  }

  def curPoolRate: util.ArrayList[Double] = {
    readRateFlag = true
    // get all threads rates
    while (RateCount.get() != curThreadsNum) { Thread.sleep(1000) }

    val list = new util.ArrayList[Double]()
    writeThreadRate.synchronized({
      writeThreadRate.valuesIterator.foreach(list.add)
   //   writeThreadRate.keysIterator.foreach(writeThreadRate(_) = 0.0)
    })
    readRateFlag = false
    RateCount.set(0)
    list
  }

  /**
   * A new thread to write the parquet file
   * @return
   */
  private def newWriter: Runnable = {

    val writer = new Runnable() {
      private var startTime: Long = 0L
      private var packageCount: Int = 0

      private var hasRead: Boolean = false

      // write data to parquet
      private val writer = new ParquetWriterWrapper(workerActor, conf)

      // num/ms
      private def getCurrentRate: Double = {
        val rate = 1.0 * packageCount / (System.currentTimeMillis() - startTime)
        startTime = System.currentTimeMillis()
        packageCount = 0
        rate
      }

      override def run(): Unit = {
        logInfo("[Netflow] Start sub Write Parquet %d"
          .format(Thread.currentThread().getId))

        writeThreadRate.synchronized(writeThreadRate(Thread.currentThread()) = 0.0)
        startTime = System.currentTimeMillis()
        packageCount = 0

        try {
          while (true) {
            val data = bufferList.take // block when this list is empty
            if (readRateFlag && !hasRead) {
              writeThreadRate
                .synchronized(writeThreadRate(Thread.currentThread()) = getCurrentRate)

              RateCount.incrementAndGet()
              hasRead = true
            }

            // reset 'hasRead'
            if(!readRateFlag & hasRead){
              hasRead = false
            }

            packageCount += 1
            val (flowSets, packetTime) = parse(data)
            while (flowSets.hasNext) {
              val dfs= flowSets.next().getRows
              writer.write(dfs, packetTime)
            }

          }
        } catch {
          case e: InterruptedException =>
            logError(e.getMessage)
            e.printStackTrace()
          case e: Exception =>
            logError(e.getMessage)
            e.printStackTrace()
        } finally {
          writer.close()
          logError("load server is closed!!!")
        }
      }
    }
    writer
  }
}
