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

import cn.ac.ict.acs.netflow.load.worker.{RowHeader, Row, MutableRow}

/**
 * Created by ayscb on 15-6-11.
 */

/**
 * data flow set
 * ----------- width (2Byte) ------------
 * |       flowsetID ( =templateID )    |  headLen_part1
 * |           flowsetLength            |  + headLen_part2 = total 4Byte
 * ----------------body------------------
 * |           record 1 field 1         |
 * |           record 1 field 2         |
 * |              .....                 |
 * |           record 1 field n         |
 * --------------------------------------
 * |           record 2 field 1         |
 * |           record 2 field 2         |
 * |              .....                 |
 * |           record 2 field n         |
 * --------------------------------------
 * |           record m field 1         |
 * |           record m field 2         |
 * |              .....                 |
 * |           record m field n         |
 * --------------------------------------
 */
class DataFlowSet(val bb: ByteBuffer, val packetTime: Long, val routerIp: Array[Byte]) {
  private val fsHeaderLen = 4

  private var dfsStartPos = 0
  private var dfsEndPos = 0
  private var existTmp: Boolean = _

  private var template : Template = _
  private def fsId = bb.getShort(dfsStartPos)
  private def fsLen = bb.getShort(dfsStartPos + 2)
  private def fsBodyLen = fsLen - fsHeaderLen

  /**
   * Get next data flow set position.
   * @param startPos
   * @return
   */
  def getNextDfS(startPos: Int): Int ={
    dfsStartPos = startPos
    dfsEndPos = startPos + fsLen

  //  println(s"[getNextDfS] startPos:${dfsStartPos}, endpos: ${dfsEndPos}, fsid:${fsId}")
    val tempKey = new TemplateKey(routerIp, fsId)
    existTmp = PacketParser.templates.containsKey(tempKey)
    if(existTmp){
      template = PacketParser.templates.get(tempKey)
    }
    dfsEndPos
  }

  def getRows: Iterator[Row] = {

    new Iterator[Row] {
      var curRow = new MutableRow(bb, template)
      if(routerIp.length == 4){
        curRow.setHeader(new RowHeader(Array[Any](packetTime, routerIp, null)))
      }else if(routerIp.length == 16){
        curRow.setHeader(new RowHeader(Array[Any](packetTime, null, routerIp)))
      }else{
        existTmp = false    // skip the package
      }

      var curRowPos : Int = dfsStartPos + fsHeaderLen

      def hasNext: Boolean = {
        if(!existTmp) return false
        if(curRowPos == dfsEndPos) return false
        true
      }

      def next() = {
        curRow.update(curRowPos)
        curRowPos += template.rowLength
        curRow
      }
    }
  }
}

