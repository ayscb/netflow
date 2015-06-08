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
package cn.ac.ict.acs.netflow.load.util

import java.io.{FileInputStream, DataInputStream, FileWriter}
import java.nio.ByteBuffer
import java.util
import java.util.concurrent.SynchronousQueue

import cn.ac.ict.acs.netflow.NetFlowConf

/**
 * analysis the data from the bytes array
 * Created by ayscb on 2015/4/13.
 */
object ParseFlowData {

  // 5C F9 DD 1E 35 76 Start mac
  private val beginFlag: Array[Byte] = Array[Byte](92, -7, -35, 30, 53, 118)

  def findNextRecord(data: DataInputStream,
    ResuseArray: Array[Byte],
    netflowBuffer: ByteBuffer): Int = {
    val macLength = 64 // min mac length
    var find = false
    while (!find) {
      while (data.available() > macLength && data.readByte() != beginFlag(0)) {}
      if (data.available() < macLength) {
        data.close()
        return 0 // the data pack is over
      }
      ResuseArray(0) = 92
      data.read(ResuseArray, 1, 5)
      if (util.Arrays.equals(beginFlag, ResuseArray)) find = true
    }

    // find the record
    upPackMac(data, netflowBuffer)
  }

  // need to jump MAC package
  private def upPackMac(data: DataInputStream, netflowBuffer: ByteBuffer): Int = {
    // mac format
    // ---------------------------------------
    // src MAC  : 6Byte
    // dest MAC : 6Byte
    // type     : 2Byte
    // body     : 46B ~ 1500B
    // CRC      : 4Byte
    // -----------------------------------------

    data.skip(6) // dest MAC
    val s = data.readShort() // type
    s match {
      case 0x0800 => upPackIP(data, netflowBuffer)
      case _ => -1 // unexpected
    }
  }

  // jump the Ip package
  private def upPackIP(data: DataInputStream, netflowBuffer: ByteBuffer): Int = {
    val headerLen = (data.readByte() & 0x0f) * 4 // 1Byte ( Ip version + headerLength )
    data.skipBytes(1) // 1Byte
    val ipTotalLength = data.readUnsignedShort() // 2Byte
    val ipBodyLength = ipTotalLength - headerLen
    data.skip(headerLen - 4)
    if (data.available() >= ipBodyLength) {
      data.readFully(netflowBuffer.array(), 0, ipBodyLength)
      netflowBuffer.limit(ipBodyLength)
      upPackUDP(netflowBuffer)
      1
    } else {
      -1 // unexpected
    }
  }

  private def upPackUDP(data: ByteBuffer): Unit = {
    // udp format
    // ---------------------------------------
    // src port : 16bit | dest port : 16bit
    // dupLength: 16bit | checksum  : 16bit
    // body ........
    // -----------------------------------------

    data.position(4) // skip src + desc port
    //   val udpBodyLength = data.getShort - 8 // header 8B
    data.position(8) // skip all the udp
  }

  def main(args: Array[String]) {
    val fs = new DataInputStream(new FileInputStream("/home/ayscb/202.97.32.135.pcap"))
    val conf = new NetFlowConf()
    val w = new ParseFlowData(conf)
    w.analysisStream(fs)
    w.closeWriter()
  }
}

class ParseFlowData(val netflowConf: NetFlowConf) {

  // for analysis the file  ( thread !!)
  private val reuse: Array[Byte] = new Array[Byte](6)
  private val netflowDataBuffer = ByteBuffer.allocate(1500)
  // receive the udp package
  private val writeUtil = new NetFlowWriterUtil(netflowConf) // for write

  def parseNetflow(data: ByteBuffer): Unit = {
    val netflowVersion = data.getShort
    netflowVersion match {
      case 5 => V5Parser.parseNetflow(writeUtil,data)
      case 9 => V9Parser.parseNetflow(writeUtil,data)
      case _ =>
    }
  }

  def analysisStream(data: DataInputStream): Unit = {
    while (data.available() > 0 &&
      ParseFlowData.findNextRecord(data, reuse, netflowDataBuffer) != 0) {
      parseNetflow(netflowDataBuffer)
      data.skipBytes(4) //  skip 4 byte mac CRC, and jump into next while cycle
    }
  }

  def closeWriter() = writeUtil.closeParquetWriter()

}
