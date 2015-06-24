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

import java.io.{ DataInputStream, DataOutputStream, FileInputStream, FileOutputStream }
import java.nio.ByteBuffer
import java.util

/**
 * Created by ayscb on 15-6-13.
 */
object loadTestFile {
  // 5C F9 DD 1E 35 76 Start mac
  private val beginFlag: Array[Byte] = Array[Byte](92, -7, -35, 30, 53, 118)

  private val writeData = new DataOutputStream(new FileOutputStream("/home/ayscb/netflow.data"))
  private val writeTemp = new DataOutputStream(new FileOutputStream("/home/ayscb/netflow.temp"))
  private val writeCom = new DataOutputStream(new FileOutputStream("/home/ayscb/netflow.com"))
  private var count = 0

  private var c = 0
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
      upPackUDP(data, netflowBuffer)
    } else {
      -1 // unexpected
    }
  }

  // udp format
  // ---------------------------------------
  // src port : 16bit | dest port : 16bit
  // dupLength: 16bit | checksum  : 16bit
  // body ........
  // -----------------------------------------

  private def upPackUDP(data: DataInputStream, netflowBuffer: ByteBuffer): Int = {
    c = c + 1
    data.skipBytes(4)
    val len = data.readShort() - 8 // except 8 byte's udp header
    data.skipBytes(2)

    if (len >= 1480) {
      udpCount += 1
      println(c + " " + len)
      data.skipBytes(1480)
      return -1
    }

    data.read(netflowBuffer.array(), 0, len)
    netflowBuffer.limit(len)
    1
  }
  private var udpCount = 0
  private def upPackNetflow(data: ByteBuffer): Unit = {

    // header 4B * 5 =20
    // FlowSet 2Byte
    if (data.getShort(0) != 9) {
      udpCount += 1
      println(s"$udpCount---> not netflow package")

      return
    }
    val no = data.getShort(20 + data.position())
    if (no < 256) {
      count += 1
      //  println(s"current templeate index $count")
      writeTemp.writeShort(data.limit())
      writeTemp.write(data.array(), data.position(), data.remaining())
    } else {
      count += 1
      val length = data.getShort(20 + data.position() + 2) // sizeof(FlowSet) = 2Byte
      if (length == data.limit() - data.position() - 20) {
        // only has data flowset
        writeData.writeShort(data.limit())
        writeData.write(data.array(), data.position(), data.remaining())
      } else {
        // maybe has template dataSet
        writeCom.writeShort(data.limit())
        writeCom.write(data.array(), data.position(), data.remaining())
        //      println(s"com --> $count")
      }
    }
  }

  def main(args: Array[String]) {
    val data = new DataInputStream(new FileInputStream("/home/ayscb/202.97.32.135.pcap"))
    val reuse: Array[Byte] = new Array[Byte](6)
    val netflowDataBuffer = ByteBuffer.allocate(1600) // receive the udp package

    while (data.available() > 0) {
      if (findNextRecord(data, reuse, netflowDataBuffer) == 1) {
        upPackNetflow(netflowDataBuffer)
        netflowDataBuffer.clear()
      }
    }

    writeData.flush()
    writeData.close()

    writeTemp.flush()
    writeTemp.close()
  }
}
