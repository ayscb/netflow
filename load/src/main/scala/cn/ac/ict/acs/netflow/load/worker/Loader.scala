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
package cn.ac.ict.acs.netflow.load.worker

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import cn.ac.ict.acs.netflow.NetFlowConf

trait Writer {
  def init()
  def write(rowIter: Iterator[Row])
  def close()
}

class ParquetWriter extends Writer {
  def init() = ???

  def write(rowIter: Iterator[Row]) = ???

  def close() = ???
}

trait WriterWrapper {
  def init()
  def write(rowIter: Iterator[Row], packetTime: Long)
  def close()
}

class ParquetWriterWrapper extends WriterWrapper {

  var w1: Writer = _
  var w2: Writer = _

  def init() = ???

  def write(rowIter: Iterator[Row], packetTime: Long) = ???

  def close() = ???
}

class Loader(packetBuffer: WrapBufferQueue, conf: NetFlowConf) extends Runnable {

  var writerWrapper: WriterWrapper = _

  override def run(): Unit = {
    while(true) {
      val currentPacket = packetBuffer.take

      val (flowSets, packetTime) = PacketParser.parse(currentPacket)
      val rows: Iterator[Row] = flowSets.flatMap(_.getRows)

      writerWrapper.write(rows, packetTime)
    }
  }

}

object PacketParser {
  
  val templates = new ConcurrentHashMap[RouterTemplate, Template]

  /**
   *
   * @param packet
   * @return (Iterator[FlowSet] , PacketTime)
   */
  def parse(packet: ByteBuffer): (Iterator[DataFlowSet], Long) = {
    // read templates
    // 1. router ip
    val packetTime = 0

    def insertTemplate(templateFlowSet: TemplateFlowSet): Unit = {
      // insert template into templates
    }

    val dataflowSet = new Iterator[DataFlowSet] {

      var currentDS: DataFlowSet = _

      def hasNext = {
        // update currentDS
        false
      }

      def next() = currentDS
    }
    (dataflowSet, packetTime)
  }
}

case class RouterTemplate(routerIp: String, templateId: Int)

case class Template

abstract class FlowSet {
  def bb: ByteBuffer
  def start: Int
}

case class DataFlowSet(bb: ByteBuffer, start: Int) extends FlowSet {
  def getRows: Iterator[Row] = {

    new Iterator[Row] {
      var curRow: Row = _

      def hasNext = ???

      def next() = curRow
    }
  }
}

case class TemplateFlowSet(bb: ByteBuffer, start: Int) extends FlowSet {

}

case class Row(bb: ByteBuffer, start: Int, length: Int, template: Template)