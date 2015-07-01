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
import java.util.concurrent.ConcurrentHashMap

import cn.ac.ict.acs.netflow.NetFlowException

object PacketParser {
  val templates = new ConcurrentHashMap[TemplateKey, Template]

  /**
   *
   * @param packet
   * @return (Iterator[FlowSet] , PacketTime)
   */
  // def parse(packet: ByteBuffer): (Iterator[DataFlowSet], Long) = {
  def parse(packet: ByteBuffer): (Iterator[DataFlowSet], Long) = {

    // 1. router ip
    val routerIp = {
      val ipLen = if (packet.get() == 4) 4 else 16
      val ip = new Array[Byte](ipLen)
      packet.get(ip)
      ip
    }

    val packetStart = packet.position() // after ip, this position is netflow header pos
    val nfVersion = packet.getShort(packetStart)

    nfVersion match {
      case 5 =>

        val nfTime = System.currentTimeMillis()
        // nfParser.getTime(packet, packetStart)

        // 2. skip to netflow body position
        val bodyStart = V5Parser.getBodyPos(packetStart)

        val dfsIter = new Iterator[DataFlowSet]() {
          private var first = true
          def hasNext: Boolean =  if (first) { first = false; true } else false
          def next(): DataFlowSet = {
            val dfs = new DataFlowSet(packet, nfTime, routerIp, 5)
            dfs.update(bodyStart, packet.limit(), V5Parser.temp)
          }
        }

        (dfsIter, nfTime)

      case 9 =>

        val nfTime = System.currentTimeMillis()
        // nfParser.getTime(packet, packetStart)

        // 2. skip to netflow body position
        val bodyStart = V9Parser.getBodyPos(packetStart)

        val dfsIter = new Iterator[DataFlowSet]() {

          private val curDFS: DataFlowSet = new DataFlowSet(packet, nfTime, routerIp, 9)
          private var curStartPos: Int = bodyStart
          private var curEndPos: Int = 0
          private var curTemp: Template = null

          override def hasNext: Boolean = {

            // TODO we remove the condition "dataFSCount != totalDataFSCount".
            if (curStartPos == packet.limit()) return false

            var lastPos = 0
            while (lastPos != curStartPos) {
              lastPos = curStartPos

              val fsId = packet.getShort(curStartPos)
              val fsLen = packet.getShort(curStartPos + 2)

              if (fsId == 0) { // template flow set

                // Cisco defines 0 as the template flowset, 1 as the option template flowset,
                // While Internet Engineering Task Force(IEIF) defines the range from
                // 0 to 255(include) as template flowset
                val stopPos = curStartPos + fsLen
                var curPos = curStartPos + 4

                while (curPos != stopPos) {
                  val tempId = packet.getShort(curPos); curPos += 2
                  val tempFields = packet.getShort(curPos); curPos +=2
                  val tempKey = new TemplateKey(routerIp, tempId)
                  val template = new Template(tempId, tempFields, packet, curPos)
                  templates.put(tempKey, template)
                  curPos += tempFields * 4
                }
              } else if (fsId == 1) { // jump this flow set
                curStartPos += fsLen
              } else if (fsId > 255) { // data flow set
                val tempId = packet.getShort(curStartPos)
                curTemp = templates.get(TemplateKey(routerIp, tempId))
                if (curTemp == null) {
                  curStartPos += fsLen
                }
              }
            }

            if (curStartPos == packet.limit()) false else true
          }

          override def next() = {
            curEndPos = curStartPos + packet.getShort(curStartPos + 2)
            curDFS.update(curStartPos, curEndPos, curTemp)
          }
        }
        (dfsIter, nfTime)

      case _ =>
        (Iterator.empty[DataFlowSet], 0)
    }
  }
}
