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
import org.apache.hadoop.fs.{ PathFilter, FileSystem, Path }
import parquet.column.ParquetProperties.WriterVersion
import parquet.hadoop.ParquetWriter
import parquet.hadoop.metadata.CompressionCodecName
import cn.ac.ict.acs.netflow.load.LoadConf
import cn.ac.ict.acs.netflow.{ Logging, NetFlowConf }
import cn.ac.ict.acs.netflow.util.TimeUtils

object NetFlowWriterUtil {

  private var maxLoad: Float = MemoryManager.DEFAULT_MEMORY_POOL_RATIO
  private var memoryManager: MemoryManager = _

  private def initMemoryManager(conf: NetFlowConf): Unit = synchronized {
    if (memoryManager == null) {
      maxLoad =
        conf.getFloat(LoadConf.MEMORY_POOL_RATIO, MemoryManager.DEFAULT_MEMORY_POOL_RATIO)

      val minAllocation =
        conf.getLong(
          LoadConf.MIN_MEMORY_ALLOCATION, MemoryManager.DEFAULT_MIN_MEMORY_ALLOCATION)

      memoryManager =
        new MemoryManager(maxLoad, minAllocation, writeSupport.getSchema)
    }
  }

  private val host = InetAddress.getLocalHost.getHostName
  private lazy val netFlowConf = new NetFlowConf(false)
  private lazy val hadoopConf = {
    val conf = new Configuration(false)
    conf.set("fs.defaultFS", "hdfs://localhost:8020")
    conf
  }

  val compress =
    CompressionCodecName.fromConf(
      netFlowConf.get(LoadConf.COMPRESSION, CompressionCodecName.UNCOMPRESSED.name()))
  val blockSize =
    netFlowConf.getInt(LoadConf.BLOCK_SIZE, ParquetWriter.DEFAULT_BLOCK_SIZE)
  val pageSize =
    netFlowConf.getInt(LoadConf.PAGE_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE)
  val enableDictionary =
    netFlowConf.getBoolean(LoadConf.ENABLE_DICTIONARY,
      ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED)
  val dictionaryPageSize =
    netFlowConf.getInt(LoadConf.DICTIONARY_PAGE_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE)
  val vilidating =
    netFlowConf.getBoolean(LoadConf.PAGE_SIZE, ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED)
  val writerVersion =
    WriterVersion.fromString(
      netFlowConf.get(LoadConf.WRITER_VERSION, WriterVersion.PARQUET_1_0.toString))
  val MemoryManageEnable =
    netFlowConf.getBoolean(LoadConf.ENABLE_MEMORYMANAGER, defaultValue = false)

  // *************************************************************************************
  private lazy val writeSupport = new DataFlowWriteSupport()
  private val fileIdx = new AtomicInteger()

  def apply(netflowConf: NetFlowConf) = new NetFlowWriterUtil(netflowConf)
}

// TODO ? may has bug
class NetFlowWriterUtil(val netflowConf: NetFlowConf) extends Logging {

  private val M = 2
  private val writers = new Array[ParquetWriter[NetflowGroup]](M)
  private var closeBoundary: Long = 0
  private val times = new Array[Long](M) // 3 times boundary

  private var curWriterIdx = 0 // should be 0 or 1
  private var startPos = 0

  private val dicInterValTime = netflowConf.getInt(TimeUtils.LOAD_DIR_CREATION_INTERVAL, 600)
  private val closeInterval = netflowConf.getInt("netflow.close.interval", 180) // default 180s
  assert(dicInterValTime > closeInterval)

  private val regularTime = Math.min((dicInterValTime + closeInterval) / 2, dicInterValTime / 2)
  private var hasClosed = false

  private var changeWriter = false

  def closeParquetWriter(): Unit = {
    if (curWriterIdx == startPos) {
      writers(curWriterIdx).close()
    } else {
      writers(curWriterIdx).close()
      writers(startPos).close()
    }

    if (NetFlowWriterUtil.memoryManager != null) {
      NetFlowWriterUtil.memoryManager.removeWriter(writers(curWriterIdx))
      if (curWriterIdx != startPos) {
        NetFlowWriterUtil.memoryManager.removeWriter(writers(startPos))
      }
    }

    // remove the file
    // workPath 2015/1/1/1/00/_temp/host-01.parquet => 2015/1/1/1/00/host-01.parquet
    val closeFilepath = getFilePath(times(curWriterIdx))
    val fileName = closeFilepath.getName // fileName = host-01.parquet
    val workBase = closeFilepath.getParent.getParent // workBase = 2015/1/1/1/00/

    val fs = workBase.getFileSystem(new Configuration())
    fs.rename(closeFilepath, new Path(workBase, fileName))
    fs.close()

  }

  def getNetflowWriter(time: Long): Option[ParquetWriter[NetflowGroup]] = {
    if (times.head == 0) {
      // first times to call method
      var checked = false

      // get the interval time
      val baseTime = TimeUtils.getCurrentBastTime(time, netflowConf)
      val interValTime = netflowConf.getInt(TimeUtils.LOAD_DIR_CREATION_INTERVAL, 600)
      for (i <- times.indices) {
        times(i) = baseTime + i * interValTime
      }

      // init the writers
      for (i <- writers.indices) {
        val workPath = getFilePath(times(i))
        if (!checked) {
          //   checkHDFSFile(workPath)
          checked = true
        }
        writers(i) = initParquetWriter(times(i))
      }
      Some(writers.head)
    } else {
      time match {
        case x if x < times(startPos) => None // do nothing
        case x if x < times(getIdx(startPos + 1)) => Some(writers(curWriterIdx))
        case x if x < closeBoundary =>
          regularCloseParquet()
          curWriterIdx = curWriterIdx ^ startPos
          Some(writers(curWriterIdx))
        case _ =>
          ImmediatelyCloseParquet()
          Some(writers(curWriterIdx))
      }
    }
  }

  private def getIdx(pos: Int): Int = pos % M

  private def closeParquet(): Unit = {
    val closeIdx: Int = startPos
    val closeFilepath = getFilePath(times(closeIdx))

    startPos = getIdx(startPos + 1)
    assert(startPos == curWriterIdx)

    times(closeIdx) = times(curWriterIdx) + dicInterValTime
    closeBoundary += dicInterValTime

    writers(closeIdx).close()
    if (NetFlowWriterUtil.memoryManager != null) {
      NetFlowWriterUtil.memoryManager.removeWriter(writers(closeIdx))
    }
    logInfo(s"[Netflow] File $closeFilepath write over!")

    // remove the file
    // workPath 2015/1/1/1/00/_temp/host-01.parquet => 2015/1/1/1/00/host-01.parquet
    val fileName = closeFilepath.getName // fileName = host-01.parquet
    val workBase = closeFilepath.getParent.getParent // workBase = 2015/1/1/1/00/

    val fs = workBase.getFileSystem(new Configuration())
    fs.rename(closeFilepath, new Path(workBase, fileName))
    fs.close()

    // init
    writers(closeIdx) = initParquetWriter(times(closeIdx))
  }

  private def ImmediatelyCloseParquet(): Unit = {
    new Thread("Close-thread") {
      override def run(): Unit = {
        if (!hasClosed) {
          hasClosed = true
          closeParquet()
        }
      }
    }.start()
  }

  private def regularCloseParquet(): Unit = {
    new Thread("Close-thread") {
      new Thread("Close-thread") {
        override def run(): Unit = {
          Thread.sleep(regularTime * 1000)
          if (!hasClosed) {
            hasClosed = true
            closeParquet()
          }
        }
      }.start()
    }
  }

  private def getFilePath(time: Long): Path = {
    val basePathTime = TimeUtils.getTimeBasePathBySeconds(time, netflowConf)
    val num = NetFlowWriterUtil.fileIdx.getAndIncrement
    val numStr = if (num < 10) "0" + num.toString else num.toString
    val filestr = NetFlowWriterUtil.host + "-" + numStr + ".parquet"

    new Path(new Path(basePathTime, LoadConf.TEMP_DICTIONARY), filestr)
  }

  private def initParquetWriter(time: Long): ParquetWriter[NetflowGroup] = {
    log.info(s"[NetFlow: Parquet block size(MB) : %d MB ] "
      .format(NetFlowWriterUtil.blockSize / 1024 / 1024))
    log.info(s"[NetFlow: Parquet page size(KB) : %d KB ] "
      .format(NetFlowWriterUtil.pageSize / 1024))
    log.info(s"[NetFlow: Parquet dictionary page size :" +
      s" ${NetFlowWriterUtil.dictionaryPageSize} ]")
    log.info(s"[NetFlow: Dictionary is ${NetFlowWriterUtil.enableDictionary} ]")
    log.info(s"[NetFlow: Validation is ${NetFlowWriterUtil.vilidating} ]")
    log.info(s"[NetFlow: Writer version is ${NetFlowWriterUtil.writerVersion} ]")

    val writer = new ParquetWriter[NetflowGroup](
      getFilePath(time), NetFlowWriterUtil.writeSupport, NetFlowWriterUtil.compress,
      NetFlowWriterUtil.blockSize, NetFlowWriterUtil.pageSize,
      NetFlowWriterUtil.dictionaryPageSize, NetFlowWriterUtil.enableDictionary,
      NetFlowWriterUtil.vilidating, NetFlowWriterUtil.writerVersion,
      NetFlowWriterUtil.hadoopConf)

    if (NetFlowWriterUtil.MemoryManageEnable) {
      NetFlowWriterUtil.initMemoryManager(netflowConf)
      if (NetFlowWriterUtil.memoryManager.getMemoryPoolRatio != NetFlowWriterUtil.maxLoad) {
        log.warn(("The configuration [ %s ] has been set. " +
          "It should not be reset by the new value: %f").
          format(LoadConf.MEMORY_POOL_RATIO, NetFlowWriterUtil.maxLoad))
      }
      NetFlowWriterUtil.memoryManager.addWriter(writer, NetFlowWriterUtil.blockSize)
    }
    writer
  }

  // we should check if the file exist in the HDFS
  // and delete if they are exist
  private def checkHDFSFile(fileName: Path) {
    val fs = FileSystem.get(NetFlowWriterUtil.hadoopConf)
    if (fs == null) {
      logError(s"[Netfloe] Can not connect with HADOOP!")
      return
    }
    val s = fs.listStatus(fileName.getParent)
    val ffs = fs.listStatus(fileName.getParent, new PathFilter {
      override def accept(path: Path): Boolean = {
        !path.getName.startsWith(NetFlowWriterUtil.host)
      }
    })

    ffs.foreach(file => {
      assert(file.isFile)
      fs.delete(file.getPath, true)
    })
  }
}
