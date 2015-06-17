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
package cn.ac.ict.acs.netflow.load.worker.parquet

import java.util.concurrent._

import akka.actor.ActorRef
import cn.ac.ict.acs.netflow.NetFlowConf
import cn.ac.ict.acs.netflow.load.LoadConf
import cn.ac.ict.acs.netflow.load.LoadMessages.CloseParquet
import cn.ac.ict.acs.netflow.load.worker.{Row, Writer, WriterWrapper}
import cn.ac.ict.acs.netflow.util.TimeUtils

class ParquetWriterWrapper(conf: NetFlowConf) extends WriterWrapper {

  private var workerActor: ActorRef = _
  private var w1: Writer = _
  private var w2: Writer = _

  private val dicInterValTime = conf.getInt(TimeUtils.LOAD_DIR_CREATION_INTERVAL, 120)
  private val closeInterval = conf.getInt(LoadConf.CLOSE_INTERVAL, 30) // default 180s
  require(dicInterValTime > closeInterval,
    "closeInterval should be less than dicInterValTime in netflow configure file")

  private var curWriterId = 0             // point to w1 or w2

  private var currTimeBoundary: Long = 0
  private var nextTimeBoundary: Long = 0

  // Here we will have two automatic threads to exec 'parquet.close()' method.
  // This 'stopLodWriter' is always point to the former thread,
  // since these two thread are started by time sequence.
  @volatile private var stopOldWriter = false

  override def init(): Unit = {}

  override def write(rowIter: Iterator[Row], packetTime: Long) : Unit = {
    if(currTimeBoundary == 0){
      currTimeBoundary = TimeUtils.getCurrentBastTime(packetTime, conf)
      nextTimeBoundary = currTimeBoundary + dicInterValTime * 1000
      w1 = TimelyParquetWriter(currTimeBoundary,conf)
      w1.write(rowIter)
      return
    }

    if(packetTime > nextTimeBoundary){
      w1.close()
      currTimeBoundary = TimeUtils.getCurrentBastTime(packetTime, conf)
      nextTimeBoundary = currTimeBoundary + dicInterValTime * 1000
      w1 = TimelyParquetWriter(currTimeBoundary,conf)
      w1.write(rowIter)
      return
    }

    w1.write(rowIter)
  }
  /**
   * close current parquet writer automatically
   * when after (currentTime + dicInterValTime + closeInterval)
   * @param rowIter
   * @param packetTime
   */
  def writes(rowIter: Iterator[Row], packetTime: Long) = {
    if (w1 == null && w2 == null) {
      currTimeBoundary = TimeUtils.getCurrentBastTime(packetTime, conf)
      w1 = TimelyParquetWriter(currTimeBoundary, conf)
      curWriterId = 0
      scheduledTask(curWriterId)
      stopOldWriter = true
      nextTimeBoundary = currTimeBoundary + dicInterValTime
    }

    packetTime match {
      case x if x > nextTimeBoundary =>
        getNextWriter(packetTime).write(rowIter)
        scheduledTask(curWriterId)            // register current writer
        stopOldWriter = false

        currTimeBoundary = nextTimeBoundary
        nextTimeBoundary += dicInterValTime

      case x if currTimeBoundary < x && x < nextTimeBoundary =>
        (if (curWriterId == 0) w1 else w2).write(rowIter)

      case x if currTimeBoundary - dicInterValTime < x && x < currTimeBoundary =>
        if (!stopOldWriter) {
          val lastWriter = if (curWriterId == 0) w2 else w1
          require(lastWriter != null)
          lastWriter.write(rowIter)
        } else {
          //TODO put the data into list
        }

      case _ =>
        //TODO put the data into list
    }
  }

  override def close() = {
    if (curWriterId == 0) {
      w1.close()
      w1 = null
    } else {
      w2.close()
      w2 = null
    }
  }

  override def setActorRef(workerRef: ActorRef): Unit = {
    workerActor = workerRef
  }

  private def getNextWriter(packetTime: Long): Writer = {
    curWriterId = (curWriterId + 1) % 2
    if (curWriterId == 0) {
      require(w1 == null)
      w1 = TimelyParquetWriter(packetTime, conf)
      w1
    } else {
      require(w2 == null)
      w2 = TimelyParquetWriter(packetTime, conf)
      w2
    }
  }
  
  private val sc = Executors.newScheduledThreadPool(2)
  private val scTask = new Array[ScheduledFuture[_]](2)
  private def scheduledTask(currWriterID: Int): Unit = {

    require(scTask(currWriterID) == null || scTask(currWriterID).isDone)
    scTask(currWriterID) = sc.schedule(new Runnable {
        override def run(): Unit = {
          require(!stopOldWriter)
          stopOldWriter = true
          if (currWriterID == 0) {
            w1.close()
            w1 = null
          } else {
            w2.close()
            w2 = null
          }
          workerActor ! CloseParquet(currTimeBoundary - dicInterValTime)
        }
    }, closeInterval + dicInterValTime, TimeUnit.SECONDS)
  }
}

