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

import cn.ac.ict.acs.netflow.{ Logging, NetFlowException }

object PacketParser extends Logging {
  val templates = new ConcurrentHashMap[TemplateKey, Template]

  /**
   *
   * @param packet
   * @return (Iterator[FlowSet] , PacketTime)
   */

  // def parse(packet: ByteBuffer): (Iterator[DataFlowSet], Long) = {
  def parse(packet: ByteBuffer): (Iterator[DataFlowSet], Long) = {

    try {
      // 1. router ip
      logWarning(s"begin parser, current packet's pos ${packet.position()}")
      var curPos = 0
      val routerIp = {
        val ipLen = if (packet.get() == 4) 4 else 16
        val ip = new Array[Byte](ipLen)
        packet.get(ip)
        ip
      }
      logWarning(s"--->after routerIp. router IP ${routerIp.map(_ & 0xff).mkString(".")}, " +
        s"current packet's pos ${packet.position()}")

      // var validFlag = true
      curPos = packet.position() // after ip, this position is netflow header pos
      val nfVersion = packet.getShort(curPos)
      logWarning(s"get nVersion = $nfVersion, current packet's pos ${packet.position()}")

      val nfParser = {
        nfVersion match {
          case 5 => V5Parser
          case 9 => V9Parser
          case _ =>
            throw new NetFlowException(s"unknown packet version. $nfVersion, " +
              s"router IP ${routerIp.map(_ & 0xff).mkString(".")} ")

        }
      }
      logWarning(s"--->after nfParser nfParser = ${nfParser.getVersion}")

      // val totalDataFSCount = nfParser.getFlowCount(packet, curPos)
      val nfTime = System.currentTimeMillis()
      // nfParser.getTime(packet, curPos)

      // 2. skip to netflow body position
      curPos = nfParser.getBodyPos(packet, curPos)
      logWarning(s"--->after curPos, current packet's pos ${packet.position()} ")

      val dataflowSet: Iterator[DataFlowSet] = new Iterator[DataFlowSet]() {

        private val curDFS: DataFlowSet =
          new DataFlowSet(packet, nfTime, routerIp, nfParser.getVersion)

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

        def getNextDfS(startPos: Int): Int = {
          dfsStartPos = startPos
          dfsEndPos = startPos + fsLen

          //  println(s"[getNextDfS] startPos:${dfsStartPos}, endpos: ${dfsEndPos}, fsid:${fsId}")
          val tempKey: TemplateKey = version match {
            case 9 => TemplateKey(routerIp, fsId)
            case _ => TemplateKey(null, version)
          }

          existTmp = PacketParser.templates.containsKey(tempKey)
          if (existTmp) {
            template = PacketParser.templates.get(tempKey)
          }
          dfsEndPos
        }


      }
      logWarning(s"--->after iterator, current packet's pos ${packet.position()} ")
      (dataflowSet, nfTime)
    } catch {
      case e: NetFlowException =>
        logError(s"${e.getMessage}")
        logError(e.getStackTrace.toString)
        logError(s"current data ${packet.array().map(x => x & 0xff).mkString(" ")}")

        (new Iterator[DataFlowSet]() {
          override def hasNext: Boolean = false
          override def next(): DataFlowSet = ???
        }, 0)

      case e: Exception =>
        logError(s"${e.getMessage}")
        logError(e.getStackTrace.toString)
        logError(s"current data ${packet.array().map(x => x & 0xff).mkString(" ")}")

        (new Iterator[DataFlowSet]() {
          override def hasNext: Boolean = false
          override def next(): DataFlowSet = ???
        }, 0)
    }
  }
}
