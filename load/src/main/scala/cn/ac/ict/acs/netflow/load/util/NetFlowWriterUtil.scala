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
package cn.ac.ict.acs.netflow.load.util

import java.net.InetAddress
import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import parquet.column.ParquetProperties.WriterVersion
import parquet.hadoop.ParquetWriter
import parquet.hadoop.metadata.CompressionCodecName

import cn.ac.ict.acs.netflow.load.LoadConf
import cn.ac.ict.acs.netflow.{Logging, NetFlowConf}
import cn.ac.ict.acs.netflow.util.TimeUtils

/**
 * get the parquet writer from some configure
 *
 */
object NetFlowWriterUtil{

  private var maxLoad: Float = 0.0f
  private var memoryManager: MemoryManager = _
  private def initMemoryManager(conf: NetFlowConf): Unit = synchronized {
    if (memoryManager == null) {
      val maxLoad =
        conf.getFloat(
          LoadConf.MEMORY_POOL_RATIO,
          MemoryManager.DEFAULT_MEMORY_POOL_RATIO)
      val minAllocation =
        conf.getLong(
          LoadConf.MIN_MEMORY_ALLOCATION,
          MemoryManager.DEFAULT_MIN_MEMORY_ALLOCATION)
      memoryManager =
        new MemoryManager(maxLoad, minAllocation, writeSupport.getSchema)
    }
  }

  private val host = InetAddress.getLocalHost.getHostName
  private lazy val netFlowConf = new NetFlowConf(false)
  private lazy val hadoopConf = {
    val conf = new Configuration(false)
    conf.set("fs.defaultFS","hdfs://localhost:8020")
    conf
  }

  lazy val compress =
    CompressionCodecName.fromConf(
      netFlowConf.get(LoadConf.COMPRESSION,CompressionCodecName.UNCOMPRESSED.name()))
  lazy val blockSize =
    netFlowConf.getInt(LoadConf.BLOCK_SIZE, ParquetWriter.DEFAULT_BLOCK_SIZE)
  lazy val pageSize =
    netFlowConf.getInt(LoadConf.PAGE_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE)
  lazy val enableDictionary =
    netFlowConf.getBoolean(LoadConf.ENABLE_DICTIONARY, ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED)
  lazy val dictionaryPageSize =
    netFlowConf.getInt(LoadConf.DICTIONARY_PAGE_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE)
  lazy val vilidating =
    netFlowConf.getBoolean(LoadConf.PAGE_SIZE, ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED)
  lazy val writerVersion =
    WriterVersion.fromString(
      netFlowConf.get(LoadConf.WRITER_VERSION, WriterVersion.PARQUET_1_0.toString))
  lazy val MemoryManageEnable =
    netFlowConf.getBoolean(LoadConf.ENABLE_MEMORYMANAGER, defaultValue = false)

  // *************************************************************************************
  private lazy val writeSupport = new DataFlowSingleWriteSupport()
  private val fileIdx = new AtomicInteger()

  def apply(netflowConf: NetFlowConf) = new NetFlowWriterUtil(netflowConf)

}

class NetFlowWriterUtil(val netflowConf: NetFlowConf) extends Logging {

//  lazy val compress =
//    CompressionCodecName.fromConf(
//      netflowConf.get(LoadConf.COMPRESSION,CompressionCodecName.UNCOMPRESSED.name()))
//  lazy val blockSize =
//    netflowConf.getInt(LoadConf.BLOCK_SIZE, ParquetWriter.DEFAULT_BLOCK_SIZE)
//  lazy val pageSize =
//    netflowConf.getInt(LoadConf.PAGE_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE)
//  lazy val enableDictionary =
//    netflowConf.getBoolean(LoadConf.ENABLE_DICTIONARY, ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED)
//  lazy val dictionaryPageSize =
//    netflowConf.getInt(LoadConf.DICTIONARY_PAGE_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE)
//  lazy val vilidating =
//    netflowConf.getBoolean(LoadConf.PAGE_SIZE, ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED)
//  lazy val writerVersion =
//    WriterVersion.fromString(
//      netflowConf.get(LoadConf.WRITER_VERSION, WriterVersion.PARQUET_1_0.toString))
//
//  lazy val MemoryManageEnable =
//    netflowConf.getBoolean(LoadConf.ENABLE_MEMORYMANAGER, defaultValue = false)

//  private var writer: ParquetWriter[NetflowGroup] = null
//  private var nextTime: Long = 0L
//  private var workPath: Path = _
//  private val hadoopConf = new Configuration()
//
//
//  private def initParquetWriter(file: Path): Unit = {
//    log.info(s"[NetFlow: Parquet block size(MB) : %d MB ] ".format(blockSize / 1024 / 1024))
//    log.info(s"[NetFlow: Parquet page size(KB) : %d KB ] ".format(pageSize / 1024))
//    log.info(s"[NetFlow: Parquet dictionary page size :  $dictionaryPageSize ]")
//    log.info(s"[NetFlow: Dictionary is $enableDictionary ]")
//    log.info(s"[NetFlow: Validation is $vilidating ]")
//    log.info(s"[NetFlow: Writer version is $writerVersion ]")
//
//    writer = new ParquetWriter[NetflowGroup](
//      file, NetFlowWriterUtil.writeSupport, compress,
//      blockSize, pageSize,
//      dictionaryPageSize, enableDictionary, vilidating, writerVersion, hadoopConf)
//
//    if (MemoryManageEnable) {
//      NetFlowWriterUtil.initMemoryManager(netflowConf)
//      if (NetFlowWriterUtil.memoryManager.getMemoryPoolRatio != NetFlowWriterUtil.maxLoad) {
//        log.warn(("The configuration [ %s ] has been set. " +
//          "It should not be reset by the new value: %f").
//          format(LoadConf.MEMORY_POOL_RATIO, NetFlowWriterUtil.maxLoad))
//      }
//      NetFlowWriterUtil.memoryManager.addWriter(writer, blockSize)
//    }
//  }

    private var writer: ParquetWriter[NetflowGroup] = null
    private var nextTime: Long = 0L
    private var workPath: Path = _

    private def initParquetWriter(file: Path): Unit = {
      log.info(s"[NetFlow: Parquet block size(MB) : %d MB ] ".format(NetFlowWriterUtil.blockSize / 1024 / 1024))
      log.info(s"[NetFlow: Parquet page size(KB) : %d KB ] ".format(NetFlowWriterUtil.pageSize / 1024))
      log.info(s"[NetFlow: Parquet dictionary page size :  ${NetFlowWriterUtil.dictionaryPageSize} ]")
      log.info(s"[NetFlow: Dictionary is ${NetFlowWriterUtil.enableDictionary} ]")
      log.info(s"[NetFlow: Validation is ${NetFlowWriterUtil.vilidating} ]")
      log.info(s"[NetFlow: Writer version is ${NetFlowWriterUtil.writerVersion} ]")

      writer = new ParquetWriter[NetflowGroup](
        file, NetFlowWriterUtil.writeSupport, NetFlowWriterUtil.compress,
        NetFlowWriterUtil.blockSize, NetFlowWriterUtil.pageSize,
        NetFlowWriterUtil.dictionaryPageSize, NetFlowWriterUtil.enableDictionary,
        NetFlowWriterUtil.vilidating, NetFlowWriterUtil.writerVersion, NetFlowWriterUtil.hadoopConf)

      if (NetFlowWriterUtil.MemoryManageEnable) {
        NetFlowWriterUtil.initMemoryManager(netflowConf)
        if (NetFlowWriterUtil.memoryManager.getMemoryPoolRatio != NetFlowWriterUtil.maxLoad) {
          log.warn(("The configuration [ %s ] has been set. " +
            "It should not be reset by the new value: %f").
            format(LoadConf.MEMORY_POOL_RATIO, NetFlowWriterUtil.maxLoad))
        }
        NetFlowWriterUtil.memoryManager.addWriter(writer, NetFlowWriterUtil.blockSize)
      }
    }

  private def initParquetWriter(time: Long): Unit = {
    val basePathTime = TimeUtils.getTimeBasePathBySeconds(netflowConf, time)
    workPath = new Path(
      new Path(basePathTime, LoadConf.TEMP_DICTIONARY),
      getworkFileStr)

    initParquetWriter(workPath)
    log.info(s"[ Netflow: write the file $workPath ]")
  }

  def UpdateCurrentWriter(currentTime: Long, memoryManageEnable: Boolean = false): Unit = {
    if (currentTime > nextTime) {
      nextTime = TimeUtils.getNextBaseTime(netflowConf, currentTime)

      if (writer != null) {
        closeParquetWriter()
        initParquetWriter(nextTime)
        log.info(s" [ Netflow : next time : $nextTime ] ")
      } else {
        val currBaseTime = TimeUtils.getCurrentBastTime(netflowConf, currentTime)
        initParquetWriter(currBaseTime)
        log.info(s" [ Netflow : next time : $currBaseTime ] ")
      }
    }
  }

  def writeData(data: NetflowGroup) = {
    writer.write(data)
  }

  def closeParquetWriter(): Unit = {
    writer.close()
    if (NetFlowWriterUtil.memoryManager != null) {
      NetFlowWriterUtil.memoryManager.removeWriter(writer)
    }
    writer = null

    // workPath 2015/1/1/1/00/_temp/host-01.parquet => 2015/1/1/1/00/host-01.parquet
    val fileName = workPath.getName // fileName = host-01.parquet
    val workBase = workPath.getParent.getParent // workBase = 2015/1/1/1/00/

    val fs = workBase.getFileSystem(new Configuration())
    fs.rename(workPath, new Path(workBase, fileName))
    fs.close()
  }

  private def getworkFileStr: String = {
    val num = NetFlowWriterUtil.fileIdx.getAndIncrement
    val numStr = if (num < 10) "0" + num.toString else num.toString
    NetFlowWriterUtil.host + "-" + numStr + ".parquet"
  }
}
