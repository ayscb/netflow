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

import java.io.IOException
import java.util.concurrent.atomic.AtomicInteger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.ParquetProperties.WriterVersion
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import cn.ac.ict.acs.netflow.{ Logging, NetFlowConf, load }
import cn.ac.ict.acs.netflow.load.LoadConf
import cn.ac.ict.acs.netflow.load.worker.{ Row, Writer }
import cn.ac.ict.acs.netflow.util.Utils

class TimelyParquetWriter(val timeBase: Long, val conf: NetFlowConf)
    extends Writer with Logging {
  import TimelyParquetWriter._



  val compression = CompressionCodecName.fromConf(
    conf.get(LoadConf.COMPRESSION, CompressionCodecName.UNCOMPRESSED.name()))
  val blockSize =
    conf.getInt(LoadConf.BLOCK_SIZE, ParquetWriter.DEFAULT_BLOCK_SIZE)
  val pageSize =
    conf.getInt(LoadConf.PAGE_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE)
  val enableDictionary =
    conf.getBoolean(LoadConf.ENABLE_DICTIONARY, ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED)
  val dictionaryPageSize =
    conf.getInt(LoadConf.DICTIONARY_PAGE_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE)
  val validating =
    conf.getBoolean(LoadConf.PAGE_SIZE, ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED)
  val writerVersion = WriterVersion.fromString(
    conf.get(LoadConf.WRITER_VERSION, WriterVersion.PARQUET_1_0.toString))
  val memoryManageEnable =
    conf.getBoolean(LoadConf.ENABLE_MEMORYMANAGER, defaultValue = false)
  lazy val maxLoad =
    conf.getFloat(LoadConf.MEMORY_POOL_RATIO, MemoryManager.DEFAULT_MEMORY_POOL_RATIO)
  lazy val minAllocation =
    conf.getLong(
      LoadConf.MIN_MEMORY_ALLOCATION, MemoryManager.DEFAULT_MIN_MEMORY_ALLOCATION)

  val outputFile: Path = {
    val basePath = if (timeBase > 0) {
      load.getPathByTime(timeBase, conf)
    } else {
      // we are a writer collecting unsorted packets
      load.systemBasePath + "/" + conf.get(LoadConf.UNSORTED_PACKETS_DIR, "unsorted")
    }
    val fileName = "%s-%02d-%d.parquet".
      format(Utils.localHostName(), fileId.getAndIncrement(), System.currentTimeMillis())
    new Path(new Path(basePath, LoadConf.TEMP_DIRECTORY), fileName)
  }

  val pw = new ParquetWriter[Row](outputFile,
    new RowWriteSupport, compression, blockSize, pageSize,
    dictionaryPageSize, enableDictionary, validating,
    writerVersion, conf.hadoopConfiguration)

  if (memoryManageEnable) {
    registerInMemManager(pw, blockSize, maxLoad, minAllocation)
  }

  override def init() = {}

  override def write(rowIter: Iterator[Row]) = {
    rowIter.foreach(row => pw.write(row))
  }

  override def close() = {
    pw.close()

    // move _template file to sub directory
    moveFile(outputFile, conf.hadoopConfiguration)

    if (memoryManageEnable) {
      removeFromMemManager(pw)
    }
  }

  private def moveFile(outFile: Path, conf: Configuration): Unit ={
    val basePath = outFile.getParent.getParent
    val fileName = outFile.getName
    val fs = basePath.getFileSystem(conf)
    try {
      if (!fs.rename(outFile, new Path(basePath, fileName))) {
        printf(s"Can not remove file ${outFile.toUri.toString} " +
          s"to ${basePath.toUri.toString.concat("/").concat(fileName)}")
      }
    } catch {
      case e: IOException =>
        printf(s"Close parquet file error, ${e.getMessage} ")
        printf(s"${e.getStackTrace.toString}")
    }
  }
}

object TimelyParquetWriter {

  private val fileId = new AtomicInteger(0)
  private var memoryManager: MemoryManager = _

  private def registerInMemManager(pw: ParquetWriter[_], blockSize: Int,
    maxLoad: Float, minAllocation: Long) = {

    if (memoryManager == null) {
      val columnsNum = ParquetSchema.overallSchema.getFieldCount
      synchronized {
        memoryManager = new MemoryManager(maxLoad, minAllocation, columnsNum)
      }
      memoryManager.addWriter(pw, blockSize)
    }
  }

  private def removeFromMemManager(pw: ParquetWriter[_]): Unit = {
    memoryManager.removeWriter(pw)
  }
}
