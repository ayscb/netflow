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

import cn.ac.ict.acs.netflow.{Logging, NetFlowException}

object PacketParser extends Logging{
  val templates = new ConcurrentHashMap[TemplateKey, Template]

  /**
   *
   * @param packet
   * @return (Iterator[FlowSet] , PacketTime)
   */
  // def parse(packet: ByteBuffer): (Iterator[DataFlowSet], Long) = {
  def parse(packet: ByteBuffer): (Iterator[DataFlowSet], Long) = {

    // 1. router ip
    var curPos = 0
    val routerIp = {
      val ipLen = if (packet.get() == 4) 4 else 16
      val ip = new Array[Byte](ipLen)
      packet.get(ip)
      ip
    }
    logDebug(s"router IP: " + routerIp.map(_&0xff).mkString("."))

    var validFlag = true
    curPos = packet.position() // after ip, this position is netflow header pos
    val nfVersion = packet.getShort(curPos)
    val nfParser = {
      nfVersion match {
        case 5 => V5Parser
        case 9 => V9Parser
        case _ => {
          logDebug(s"unnkow version, for $nfVersion")
          logDebug(s"data: " + packet.array().map(_&0xff).mkString(" "))
          // throw new NetFlowException("unknown packet version.")
        }
      }
    }

    val totalDataFSCount = nfParser.getFlowCount(packet, curPos)
    val nfTime = System.currentTimeMillis()
    // nfParser.getTime(packet, curPos)

    // 2. skip to netflow body position
    curPos = nfParser.getBodyPos(packet, curPos)

    val dataflowSet: Iterator[DataFlowSet] = new Iterator[DataFlowSet]() {

      private val curDFS: DataFlowSet = new DataFlowSet(packet, nfTime, routerIp)
      private var curDFSPos = curPos

      override def hasNext: Boolean = {

        // TODO we remove the condition "dataFSCount != totalDataFSCount".
        if (curDFSPos == packet.limit()) return false
        var lastPos = 0

        while (lastPos != curDFSPos) {
          lastPos = curDFSPos
          curDFSPos = nfParser.getNextFSPos(packet, curDFSPos, routerIp)
          if (curDFSPos == packet.limit()) return false
        }
        true
      }

      override def next() = {
        curDFSPos = curDFS.getNextDfS(curDFSPos)
        curDFS
      }
    }

    (dataflowSet, nfTime)
  }
}
