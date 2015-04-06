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
import scala.collection.mutable.Map
import scala.io.Source
import scala.util.Random

/**
 * template about the netflow  V9 data
 * Created by ayscb on 2015/3/29.
 */

case class Template(template: mutable.Map[Int, Int], rowBytes: Int) {

  @transient var rd: Random = new Random()


  val variableKey = Set(1, 2, 3, 10, 14, 16, 17, 19, 20,
    23, 24, 40, 41, 42, 82, 83, 84, 85, 86, 94, 100)

  // we regard  the ip mac type as var length type
  val variableKeys = variableKey ++ Set(8, 12, 15, 27, 28, 62, 63, 56, 57, 80, 81)

  def getRowLength = rowBytes

  def getRowData(currTime: Long, row: GenericMutableRow,
                 arrayContainer: Array[Array[Byte]]): Unit = {

    rd = if (rd == null) new Random(currTime) else rd
    if (template.size == 0) throw new java.lang.RuntimeException("should call praseCommand first")

    row.setLong(0, currTime) // time

    var i = 0
    for (key <- template.keySet) {
      val valLen = template.getOrElse(key, -1)

      if (variableKeys.contains(key)) {
        setVarDataLength(key, valLen, arrayContainer(i), row)
        i += 1
      } else {
        setFixeLengthData(key, valLen, row)
      }
    }
  }

  // we want to reuse the array buffer
  def getArrayContainer: Array[Array[Byte]] = {
    val arrs = new Array[Array[Byte]](variableKeys.size)
    var i = 0
    for (key <- template.keySet if variableKeys.contains(key)) {
      arrs(i) = new Array[Byte](template.getOrElse(key, 0))
      i += 1
    }
    arrs
  }

  private def setFixeLengthData(key: Int, valueLen: Int, row: GenericMutableRow): Unit = {
    valueLen match {
      case 1 =>
        val v = rd.nextInt(Template.BYTE_MAX).toShort
        row.setShort(key, v)

      case 2 =>
        val v = rd.nextInt(Template.SHORT_MAX)
        row.setInt(key, v)

      case 4 =>
        val v = Math.abs(rd.nextLong() % Template.INT_MAX)
        row.setLong(key, v)
    }
  }

  private def setVarDataLength(key: Int, valueLen: Int,
                               container: Array[Byte], row: GenericMutableRow): Unit = {
    key match {
      case (8 | 12 | 15) => getIPV4(container) // ipv4
      case (27 | 28 | 62 | 63) => getIPV6(container) // ipv6
      case (56 | 57 | 80 | 81) => getMAC(container) // mac
      case _ => getStringDataLength(template.getOrElse(key, -1), container)
    }
    row.update(key, container)
  }

  private def getStringDataLength(dataLen: Int, container: Array[Byte]): Unit = {
    if (dataLen != container.length) {
      throw new scala.RuntimeException(" expect :" + dataLen + " actual : " + container.length)
    }
    for (i <- 0 until dataLen) rd.nextBytes(container)
  }

  private def getIPV4(container: Array[Byte]): Unit = getStringDataLength(4, container)

  // 16个字节 分8组 (  FE80:0000:0000:0000:AAAA:0000:00C2:0002 )
  private def getIPV6(container: Array[Byte]): Unit = getStringDataLength(16, container)

  // 6个字节 表示的是  00-23-5A-15-99-42
  private def getMAC(container: Array[Byte]): Unit = getStringDataLength(6, container)
}

object Template {

  val BYTE_MAX = 255 / 2
  val SHORT_MAX = 65535 / 2
  val INT_MAX = 4294967295L / 2
  val IP_MAX = 256

  private val template: mutable.Map[Int, Int] = new mutable.HashMap[Int, Int]

  /** we get the length from **/
  private var rowLength = 0

  /**
   * template
   *
   * @param fileName template  file path
   */
  def load(fileName: String): Template = {

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
    Template(template, rowLength)
  }
}
