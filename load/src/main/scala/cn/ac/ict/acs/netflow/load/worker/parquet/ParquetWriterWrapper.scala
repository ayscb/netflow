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
import cn.ac.ict.acs.netflow.{ Logging, NetFlowConf, load }
import cn.ac.ict.acs.netflow.load.LoadMessages.CloseParquet
import cn.ac.ict.acs.netflow.load.worker.{ Row, Writer, WriterWrapper }

import scala.collection._

class ParquetWriterWrapper(worker: ActorRef, conf: NetFlowConf)
    extends WriterWrapper with Logging {

  private val dicInterValTime = load.dirCreationInterval(conf)
  private val closeDelay = load.writerCloseDelay(conf)
  require(dicInterValTime > closeDelay,
    "closeInterval should be less than dicInterValTime in netflow configure file")

  private val delayTime = dicInterValTime + closeDelay
  logInfo(s"totalTime: $delayTime ms. dicTime: $dicInterValTime, closeDelay: $closeDelay")

  private val timeToWriters = mutable.HashMap.empty[Long, Writer]
  private val closeWriterScheduler = mutable.HashMap.empty[Writer, ScheduledFuture[_]]

  private def registerCloseScheduler(writer: Writer) = {

    closeWriterScheduler(writer) =
      ParquetWriterWrapper.scheduledThreadPool.schedule(new Runnable {
        override def run() = {
          writer.close()
          worker ! CloseParquet(writer.timeBase())
          timeToWriters.synchronized {
            timeToWriters -= writer.timeBase()
          }
          closeWriterScheduler.synchronized {
            closeWriterScheduler -= writer
          }
          logInfo(s"Close current file ${load.getPathByTime(writer.timeBase(), conf)}")
        }
      }, delayTime, TimeUnit.MILLISECONDS)
  }

  override def init(): Unit = {}

  override def write(rowIter: Iterator[Row], packetTime: Long) = {
    val timeBase = load.getTimeBase(packetTime, conf)
    val writer = timeToWriters.getOrElseUpdate(timeBase, {
      val tbw = new TimelyParquetWriter(timeBase, conf)
      tbw.init()
      registerCloseScheduler(tbw)
      tbw
    })
    writer.write(rowIter)
  }

  override def close() = {
    closeWriterScheduler.values.foreach(_.cancel(false))
    timeToWriters.values.foreach(_.close())
  }
}

object ParquetWriterWrapper {
  val scheduledThreadPool = Executors.newScheduledThreadPool(2)
}

