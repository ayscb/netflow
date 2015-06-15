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

import cn.ac.ict.acs.netflow.load.worker.parser.PacketParser._
import cn.ac.ict.acs.netflow.{Logging, NetFlowConf}
import cn.ac.ict.acs.netflow.load.worker.parquet.ParquetWriterWrapper
import cn.ac.ict.acs.netflow.util.ThreadUtils

/**
 * Created by ayscb on 15-6-11.
 */
final class LoaderService(private val bufferList: WrapBufferQueue, private val conf: NetFlowConf)
  extends  Logging{

  // get ResolvingNetflow threads
  private val writerThreadPool = ThreadUtils.newDaemonCachedThreadPool("parquetWriterPool")
  private val writerThreadsQueue = new scala.collection.mutable.Queue[Thread]

  private val ratesQueue = new java.util.concurrent.LinkedBlockingQueue[Double]()

  @volatile private var readRateFlag = false

  def initParquetWriterPool(threadNum: Int) = {
    for (i <- 0 until threadNum) {
      writerThreadPool.submit(newWriter)
    }
  }

  def curThreadsNum: Int = writerThreadsQueue.size

  def adjustWritersNum(newThreadNum: Int) = {
    val currThreadNum = writerThreadsQueue.size
    logInfo(s"[Netflow] Current total resolving thread number is $currThreadNum, " +
      s" and will be adjust to $newThreadNum ")

    if (newThreadNum > currThreadNum) {
      // increase threads
      for (i <- 0 until (newThreadNum - currThreadNum))
        writerThreadPool.submit(newWriter)
    } else {
      // decrease threads
      writerThreadsQueue.take(currThreadNum - newThreadNum).foreach(_.interrupt())
    }
  }

  def stopAllWriterThreads() = {
    logInfo((" current threads number is %d, all " +
      "threads will be stopped").format(writerThreadsQueue.size))
    writerThreadsQueue.foreach(_.interrupt())
    writerThreadPool.shutdown()
  }

  def curPoolRate: util.ArrayList[Double] = {
    readRateFlag = true
    val currentThreadsNum = writerThreadsQueue.size
    while (ratesQueue.size() != currentThreadsNum) { Thread.sleep(1) } // get all threads rates
    val list = new util.ArrayList[Double]()
    ratesQueue.drainTo(list)
    ratesQueue.clear()
    readRateFlag = false
    list
  }

  /**
   * A new thread to write the parquet file
   * @return
   */
  private def newWriter: Runnable = {

    val writer = new Runnable() {
      private var hasRead = false
      // et true after call method 'getCurrentRate'
      private var startTime = 0L
      private var packageCount = 0

      // write data to parquet
      private val writer = new ParquetWriterWrapper(conf)

      private def getCurrentRate: Double = {
        val rate = 1.0 * packageCount / (System.currentTimeMillis() - startTime)
        startTime = System.currentTimeMillis()
        packageCount = 0
        rate
      }

      override def run(): Unit = {
        logInfo("[Netflow] Start sub Write Parquet %d"
          .format(Thread.currentThread().getId))

        writerThreadsQueue.enqueue(Thread.currentThread())
        try{
          while (Thread.currentThread().isInterrupted) {
            val data = bufferList.take // block when this list is empty

            if (readRateFlag && !hasRead) {
              // read the current rate, and put the data into queue
              ratesQueue.put(getCurrentRate)
              hasRead = true
            } else if (!readRateFlag & hasRead) {
              hasRead = false
            }
            packageCount += 1
            val (flowSets, packetTime) = parse(data)
            val rows: Iterator[Row] = flowSets.flatMap(_.getRows)
            writer.write(rows, packetTime)
          }
        }catch {
          case e : InterruptedException =>
            // print ? do nothing
        }finally {
          writer.close()
        }
      }
    }
    writer
  }
}