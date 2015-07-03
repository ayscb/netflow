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

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

object loadTestFile {

  // -----------------------------------------------------
  // |                 pcap header(24Byte)               |
  // | record1 - header(16Byte) | record1 - body(16Byte) |
  // | record2 - header(16Byte) | record1 - body(16Byte) |
  // | record3 - header(16Byte) | record1 - body(16Byte) |
  // -----------------------------------------------------

  val header_length = 24              // 24Byte   fix header length
  val record_header_len = 16          // 16Byte   a record header
  val mac_header_len = 6 + 6 + 2      // 14Byte   MAC src + MAC desc + pro
  val ip_header = 4 * 5               // 20Byte   IP header
  val udp_header = 2 + 2 + 2 + 2      // 8Byte    UDP header
  val load_len_pos = record_header_len + mac_header_len + ip_header + 2 + 2

  def main(args: Array[String]) {
    val bf = ByteBuffer.allocate(2)

    val fci: FileChannel = new RandomAccessFile("/home/ayscb/netflow5.pcap", "r").getChannel
    val fco = new FileOutputStream("/home/ayscb/netflow5").getChannel

    fci.position(header_length)     // skip file header
    praser(fci, fco)
    fci.close()
    fco.close()
  }

  /**
   * decode pcap file as format " length payLoad length payload ......."
   * @param fin
   * @param fout
   */
  def praser(fin: FileChannel, fout: FileChannel): Unit ={
    val bf = ByteBuffer.allocate(2)
    while(true){
      fin.position(fin.position() + load_len_pos)   // skip to udp length
      if(fin.read(bf) == -1 ) return                // get payload length
      fin.position(fin.position() + 2)              // skip 2 byte

      bf.flip()
      val len = bf.getShort(0) - 8                  // get real length
      bf.putShort(0,len.asInstanceOf[Short])
      fout.write(bf)
      if( fin.transferTo(fin.position(),len,fout) == 0 ) return
      fin.position(fin.position() + len)
      bf.clear()
    }
  }
}
