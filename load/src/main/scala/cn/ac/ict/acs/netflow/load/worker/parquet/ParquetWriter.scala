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

import java.util.concurrent.atomic.AtomicInteger
import org.apache.hadoop.fs.Path
import parquet.column.ParquetProperties.WriterVersion
import parquet.hadoop.ParquetWriter
import parquet.hadoop.metadata.CompressionCodecName

import cn.ac.ict.acs.netflow.NetFlowConf
import cn.ac.ict.acs.netflow.load.LoadConf
import cn.ac.ict.acs.netflow.load.worker.{Row, Writer}
import cn.ac.ict.acs.netflow.util.{Utils, TimeUtils}


class TimelyParquetWriter(file: Path, val conf: NetFlowConf) extends Writer {
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

  val pw = new ParquetWriter[Row](file,
    new RowWriteSupport, compression, blockSize, pageSize,
    dictionaryPageSize, enableDictionary, validating,
    writerVersion, conf.hadoopConfiguration)

  if(memoryManageEnable){
    registerInMemManager(pw, blockSize, conf)
  }

  override def init() = {}

  override def write(rowIter: Iterator[Row]) = {
    rowIter.foreach(row=>pw.write(row))
  }

  override def close() = {
    pw.close()
    if(memoryManageEnable){
      removeFromMemManager(pw)
    }
  }
}

object TimelyParquetWriter {

  private var memoryManager: MemoryManager = _

  private def registerInMemManager(pw: ParquetWriter[_], blockSize: Int, conf: NetFlowConf) = {
    if (memoryManager == null) {
      synchronized {
        val maxLoad =
          conf.getFloat(LoadConf.MEMORY_POOL_RATIO, MemoryManager.DEFAULT_MEMORY_POOL_RATIO)

        val minAllocation =
          conf.getLong(
            LoadConf.MIN_MEMORY_ALLOCATION, MemoryManager.DEFAULT_MIN_MEMORY_ALLOCATION)

        val columnsNum = ParquetSchema.overallSchema.getColumns.size()
        memoryManager = new MemoryManager(maxLoad, minAllocation, columnsNum)
      }
      memoryManager.addWriter(pw,blockSize)
    }
  }

  private def removeFromMemManager(pw: ParquetWriter[_] ): Unit ={
    memoryManager.removeWriter(pw)
  }

  private val fileId = new AtomicInteger(0)
  private def getFilePath(time: Long, conf: NetFlowConf): Path = {
    val basePathTime = TimeUtils.getTimeBasePathBySeconds(time, conf)
    val fileName ="%s-%02d-%d.parquet"
      .format(Utils.localHostName(), fileId.getAndIncrement(), System.currentTimeMillis())
    new Path(new Path(basePathTime, LoadConf.TEMP_DIRECTORY), fileName)
  }

  def apply(timeInMillis: Long, conf: NetFlowConf): TimelyParquetWriter ={
    val file =
      if (timeInMillis != 0) getFilePath(timeInMillis, conf) else new Path("/XXXXXX")
    new TimelyParquetWriter(file,conf)
  }
}