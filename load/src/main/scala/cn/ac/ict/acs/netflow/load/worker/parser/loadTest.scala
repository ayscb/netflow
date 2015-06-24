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

import java.io.{ DataInputStream, FileInputStream }
import java.nio.ByteBuffer

import cn.ac.ict.acs.netflow.NetFlowException
import cn.ac.ict.acs.netflow.load.worker.Row

object loadTest {

  val data = ByteBuffer.allocate(1550)

  val ip = Array(
    192.asInstanceOf[Byte],
    168.asInstanceOf[Byte],
    1.asInstanceOf[Byte],
    1.asInstanceOf[Byte])
  data.put(4.asInstanceOf[Byte])
  data.put(ip)

  val pos = data.position()

  def main(args: Array[String]) {
    var idx = 0

    var packageCount = 0
    var rowCount = 0

    var fileCount = 0
    val s = System.currentTimeMillis()

    while (idx != args.length) {
      val file = args(idx)
      val fi = new FileInputStream(file)
      val bf = new DataInputStream(fi)

      fileCount += bf.available()
      while (bf.available() != 0) {
        val length = bf.readShort()
        data.position(pos)
        bf.read(data.array(), data.position(), length)
        val limit = data.position() + length

        data.flip()
        data.limit(limit)

        try {
          packageCount += 1
          val (flowSets, packetTime) = PacketParser.parse(data)
          val rows: Iterator[Row] = flowSets.flatMap(_.getRows)
          while (rows.hasNext) {
            val row = rows.next()
            rowCount += 1
          }
        } catch {
          case e: NetFlowException => // println(e.getMessage)
        }
      }
      idx += 1
    }
    println(s" package count: $packageCount  rowCount: $rowCount")
    println(s"file : ${fileCount / 1024 / 1024}MB --time: ${System.currentTimeMillis() - s}ms})")
  }
}
