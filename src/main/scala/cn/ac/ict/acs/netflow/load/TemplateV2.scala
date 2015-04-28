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
package cn.ac.ict.acs.netflow.load

import org.apache.spark.sql.catalyst.expressions.GenericMutableRow

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Random

/**
 * template about the netflow  V9 data
 * Created by ayscb on 2015/3/29.
 */

case class TemplateV2(template: mutable.Map[Int, Int], rowBytes: Int) {

  @transient lazy val rd: Random = new Random()

  val variableKey = Set(1, 2, 3, 10, 14, 16, 17, 19, 20,
    23, 24, 40, 41, 42, 82, 83, 84, 85, 86, 94, 100)

  // we regard  the ip mac type as var length type
  val variableKeys = variableKey ++ Set(8, 12, 15, 27, 28, 62, 63, 56, 57, 80, 81)

  def getRowLength = rowBytes

  def getRowData(currTime: Long,
                 row: GenericMutableRow
                 ): Unit = {

    // rd = if (rd == null) new Random(currTime) else rd
    if (template.size == 0) throw new java.lang.RuntimeException("should call praseCommand first")

    row.setLong(0, currTime) // time

    var i = 0
    for (key <- template.keySet) {
      val valLen = template.getOrElse(key, -1)
      if (variableKeys.contains(key)) {
        setVarDataLength(key, valLen, row)
        i += 1
      } else {
        setFixeLengthData(key, valLen, row)
      }
    }
  }


  private def setFixeLengthData(key: Int, valueLen: Int, row: GenericMutableRow): Unit = {
    valueLen match {
      case 1 =>
        val v = rd.nextInt(TemplateV2.BYTE_MAX).toShort
        row.setShort(key, v)

      case 2 =>
        val v = rd.nextInt(TemplateV2.SHORT_MAX)
        row.setInt(key, v)

      case 4 =>
        val v = Math.abs(rd.nextLong() % TemplateV2.INT_MAX)
        row.setLong(key, v)
    }
  }

  private def setVarDataLength(key: Int, valueLen: Int,
                               row: GenericMutableRow): Unit = {
    val value = key match {
                  case (8 | 12 | 15) => getIPV4 // ipv4
                  case (27 | 28 | 62 | 63) => getIPV6 // ipv6
                  case (56 | 57 | 80 | 81) => getMAC // mac
                  case _ => getStringDataLength(template.getOrElse(key, -1))
                }
    row.update(key,value)
  }

  private def getStringDataLength(dataLen: Int): Array[Byte] = {
    if( dataLen == -1){
      throw new IllegalArgumentException("dataLen should be > 0 " +
        ", but now dataLen = " + dataLen)
    }

    val result = new Array[Byte](dataLen)
    rd.nextBytes(result)
    result
  }

  private def getIPV4 = getStringDataLength(4)

  // 16个字节 分8组 (  FE80:0000:0000:0000:AAAA:0000:00C2:0002 )
  private def getIPV6 = getStringDataLength(16)

  // 6个字节 表示的是  00-23-5A-15-99-42
  private def getMAC = getStringDataLength(6)
}

object TemplateV2 {

  val BYTE_MAX = 255
  val SHORT_MAX = 65535
  val INT_MAX = 4294967295L
  val IP_MAX = 256

  private val template: mutable.Map[Int, Int] = new mutable.HashMap[Int, Int]

  /** we get the length from **/
  private var rowLength = 0

  /**
   * template
   *
   * @param fileName template  file path
   */
  def load(fileName: String): TemplateV2 = {

    val file = Source.fromFile(fileName)
    if (file == null) {
      throw new java.lang.IllegalAccessError(String.format("the file %s does not exist!", fileName))
    }

    val lineIter = file.getLines()

    for (line <- lineIter) {
      val kv: Array[String] = line.split(" ")
      val valueLen = kv(1).toInt
      template += (kv(0).toInt -> valueLen)
      rowLength += valueLen
    }

    file.close()
    TemplateV2(template, rowLength)
  }
}
