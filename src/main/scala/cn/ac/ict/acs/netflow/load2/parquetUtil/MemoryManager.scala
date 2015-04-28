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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.ac.ict.acs.netflow.load2.parquetUtil


import java.lang.management.ManagementFactory
import cn.ac.ict.acs.netflow.util.Logging
import parquet.hadoop.ParquetWriter
import scala.collection.mutable

/**
 * copy from parquet.hadoop.MemoryManage
 *
 * Created by ayscb on 2015/4/20.
 */
object MemoryManager{
  val DEFAULT_MEMORY_POOL_RATIO: Float = 0.95f
  val DEFAULT_MIN_MEMORY_ALLOCATION: Long = ParquetWriter.DEFAULT_PAGE_SIZE

  private def checkRatio(ratio: Float) {
    if (ratio <= 0 || ratio > 1) {
      throw new IllegalArgumentException("The configured memory pool ratio " + ratio + " is " + "not between 0 and 1.")
    }
  }

  def apply(memoryPoolRatio : Float,
            minMemoryAllocation : Long,
            schema : parquet.schema.MessageType ): Unit ={
    checkRatio(memoryPoolRatio)
    new MemoryManager(memoryPoolRatio,minMemoryAllocation,schema)
  }

}

class MemoryManager(
                     val memoryPoolRatio : Float,
                     val minMemoryAllocation : Long,
                     val schema : parquet.schema.MessageType ) extends Logging{

  def this( schema : parquet.schema.MessageType ) =
    this(MemoryManager.DEFAULT_MEMORY_POOL_RATIO,MemoryManager.DEFAULT_MIN_MEMORY_ALLOCATION,schema )

  private val totalMemoryPool =
    ManagementFactory.getMemoryMXBean.getHeapMemoryUsage.getMax * memoryPoolRatio.round
  log.info(s"[ Netflow : Allocated total memory pool is: $totalMemoryPool ]" )

  private val writerList = new mutable.HashMap[ParquetWriter[_], Long]

  def addWriter (
                  writer : ParquetWriter[_],
                  allocation : Long) : Unit =
    synchronized{
      writerList.get(writer) match {
        case None =>writerList += ( writer -> allocation )
        case Some(x) =>
          throw new IllegalArgumentException(
            "[BUG] The Parquet Memory Manager should not add an "
            + "instance of InternalParquetRecordWriter more than once."
            +" The Manager already contains " + "the writer: " + x)
      }
      updateAllocation()
  }

  private def updateAllocation() = {
    var totalAllocations = 0L
    var scale: Double = 0.0

    writerList.values.foreach(x => totalAllocations += x)

    if (totalAllocations <= totalMemoryPool) {
      scale = 1.0
    } else {
      scale = 1.0 * totalMemoryPool / totalAllocations
      log.warn("Total allocation exceeds %.2f%% (%d bytes) of heap memory\n".format(100 * memoryPoolRatio, totalMemoryPool))
      log.warn("Scaling row group sizes to %.2f%% for %d writers".format(100 * scale, writerList.size))
    }

    var maxColCount: Int = 0
    import scala.collection.JavaConversions._

    writerList.keySet.foreach(
      x => maxColCount = Math.max(schema.getColumns.size(), maxColCount))

    writerList.entrySet.foreach(
      entry => {
        val newsize = Math.floor(entry.getValue * scale).asInstanceOf[Long]
        if (minMemoryAllocation > 0 && newsize / maxColCount < minMemoryAllocation) {
          throw new RuntimeException(
            ("New Memory allocation %d exceeds minimum " +
              "allocation size %d with largest schema having %d columns")
              .format(newsize, minMemoryAllocation, maxColCount)) {
          }

          val innerwriter = entry.getKey.getClass.getDeclaredField("writer")
          innerwriter.setAccessible(true)

          val cls = Class.forName("parquet.hadoop.InternalParquetRecordWriter")
          val ms = cls.getDeclaredMethod("setRowGroupSizeThreshold", classOf[Long])
          ms.setAccessible(true)

          ms.invoke(innerwriter.get(entry.getKey), Long.box(newsize))
          log.debug("Adjust block size from %,d to %,d for writer: %s"
            .format(entry.getValue, newsize, entry.getKey))
        }
      })
  }

  def removeWriter(  writer : ParquetWriter[_] ) = synchronized{
    if( writerList.contains(writer)) writerList.remove(writer)
    if( !writerList.nonEmpty) updateAllocation()
  }

  /**
   * Get the total memory pool size that is available for writers.
   * @return the number of bytes in the memory pool
   */
 def getTotalMemoryPool: Long = totalMemoryPool


  /**
   * Get the writers list
   * @return the writers in this memory manager
   */
def getWriterList: mutable.HashMap[ParquetWriter[_], Long] =  writerList


  /**
   * Get the ratio of memory allocated for all the writers.
   * @return the memory pool ratio
   */
 def getMemoryPoolRatio: Float = memoryPoolRatio
}
