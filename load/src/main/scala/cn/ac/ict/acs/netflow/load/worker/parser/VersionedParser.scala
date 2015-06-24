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
package cn.ac.ict.acs.netflow.load.worker.parser

import java.nio.ByteBuffer

/**
 * Created by ayscb on 15-6-11.
 */
trait VersionedParser {

  def getVersion: Int

  /**
   *
   * @param data the data contain router ip and netflow data. (single package)
   * @param startPos  start form netflow header
   * @return
   */
  def getFlowCount(data: ByteBuffer, startPos: Int): Int

  /**
   *
   * @param data the data contain router ip and netflow data. (single package)
   * @param startPos start from netflow header
   * @return
   */
  def getTime(data: ByteBuffer, startPos: Int): Long

  /**
   *
   * @param data the data contain router ip and netflow data. (single package)
   * @param startPos start from netflow header
   * @return
   */
  def getBodyPos(data: ByteBuffer, startPos: Int): Int

  /**
   *  Get the next position of the data flow set,
   *  for V9 ,we will deal with the template flow set
   * @param data the data contain router ip and netflow data. (single package)
   * @param startPos start from netflow body
   * @return
   */
  def getNextFSPos(data: ByteBuffer, startPos: Int, routerIp: Array[Byte]): Int

}

/**
 * V9 format
 * --------------header---------------
 * |    version     |   flowSetCount    |
 * |          systemUpTime              |
 * |          unixSeconds               |
 * |        packageSequence             |
 * |             sourceID               |
 * ----------------body---------------
 * |              flowSet               |
 * |              flowSet               |
 * |              ......                |
 * -----------------------------------
 */
object V9Parser extends VersionedParser {
  override def getVersion: Int = 9

  override def getFlowCount(data: ByteBuffer, startPos: Int): Int = data.getShort(startPos + 2)

  override def getTime(data: ByteBuffer, startPos: Int): Long = {
    data.getInt(startPos + 8) & 0xFFFFFFFFFFL
  }
  override def getBodyPos(data: ByteBuffer, startPos: Int): Int = startPos + 20

  override def getNextFSPos(data: ByteBuffer, startPos: Int, routerIp: Array[Byte]): Int = {

    // insert template into templates
    val flowsetId = data.getShort(startPos)
    val flowsetLen = data.getShort(startPos + 2)
    //   println(s"[call V9Parser.getNextFSPos] flowsetID:$flowsetId flowLen:$flowsetLen")

    flowsetId match {
      case x if x > 255 =>
        // data flow set
        startPos

      case 0 =>
        // Cisco defines 0 as the template flowset, 1 as the option template flowset,
        // While Internet Engineering Task Force(IEIF) defines the range from
        // 0 to 255(include) as template flowset
        val stopPos = startPos + flowsetLen
        var curPos = startPos + 4

        data.position(curPos)
        while (curPos != stopPos) {
          val tempId = data.getShort
          val tempFields = data.getShort
          val tempKey = new TemplateKey(routerIp, tempId)
          val template = new Template(tempId, tempFields).createTemplate(data)

          PacketParser.templates.put(tempKey, template)
          curPos = data.position()
        }
        stopPos

      case _ => startPos + flowsetLen
    }
  }
}

/**
 * V5 format
 * ---------------------header-------------------------
 * |        version         |   flowSetCount (1-30)  |
 * |                    sysUpTime                    |
 * |                   Epoch seconds                 |
 * |                   Nano seconds                  |
 * |                   Flows Seen                    |
 * | EngineType | EngineID |      Sampling info      |
 * ----------------------body-------------------------
 * |                    flowSet                      |
 * |                    flowSet                      |
 * |                    ......                       |
 * ---------------------------------------------------
 *
 */
object V5Parser extends VersionedParser {
  override def getVersion: Int = 5
  override def getFlowCount(data: ByteBuffer, startPos: Int): Int = data.getShort(startPos + 2)
  override def getTime(data: ByteBuffer, startPos: Int): Long = {
    data.getShort(startPos + 8) & 0xFFFFFFFFFFL
  }
  override def getBodyPos(data: ByteBuffer, startPos: Int): Int = data.getShort(startPos + 24)
  override def getNextFSPos(data: ByteBuffer, startPos: Int, routerIp: Array[Byte]): Int = startPos

}
