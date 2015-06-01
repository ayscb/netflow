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

import java.nio.ByteBuffer

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
 *
 * analysis the V9 netflow data
 */

class V9Analysis extends NetFlowAnalysis {

  override def unPackHeader(data: ByteBuffer): NetflowHeader = {
    new NetflowHeader(
      //     BytesUtil.toUShort(data),   // version
      BytesUtil.toUShort(data), // flowSetCount
      BytesUtil.toUInt(data), // systemUptime
      BytesUtil.toUInt(data), // unixSeconds
      BytesUtil.toUInt(data), // packageSequence
      BytesUtil.toUInt(data) // sourceID
      )
  }

  override def isTemplateFlowSet(data: ByteBuffer): Boolean = {
    val flowSetID = data.getShort(data.position()) & 0xFFFF
    if (flowSetID < 256) true else false
  }

  override def isTemplateExist(data: ByteBuffer): Int = {
    val flowSetID = data.getShort(data.position()) & 0xFFFF
    if (!NetFlowAnalysis.templates.contains(flowSetID)) {
      // Since the template does not exist , we do not understand the data.
      // So we should skip this flow set.
      // TODO skip the data ? or save the data
      val length = data.getShort(data.position() + 2) & 0xFFFF
      data.position(data.position + data.remaining().min(length))
      -1
    } else {
      flowSetID
    }
  }

  override def updateTemplate(data: ByteBuffer): Unit = {
    val flowSet = data.getShort(data.position())
    flowSet match {
      case 1 => updateOptionTemplate(data)
      case x if x >= 0 => updateNormalTemplate(data)
      case _ =>
    }
  }

  private def updateNormalTemplate(data: ByteBuffer): Unit = {
    val startPos = data.position() // add the flowSetID ( 2Byte )
    data.getShort // skip the flowset id
    val flowLen = BytesUtil.toUShort(data)

    while ((data.position() - startPos) != flowLen) {
      val tmpId = BytesUtil.toUShort(data)
      val fieldCount = BytesUtil.toUShort(data)

      NetFlowAnalysis.templates.get(tmpId) match {
        case Some(temp) => temp.updateTemplate(tmpId, fieldCount, data)
        case None =>
          val template = new SafeTemplate(tmpId, fieldCount)
          template.updateTemplate(tmpId, 0, data)
          NetFlowAnalysis.templates += (tmpId -> template)
      }
    }
  }

  private def updateOptionTemplate(data: ByteBuffer): Unit = {
    val startPos = data.position()
    data.getShort // skip the flowset id
    val flowLen = BytesUtil.toUShort(data)

    // TODO: do we need to save the option template ? ( skip know)
    data.position(startPos + flowLen)
    return

    val tmpId = BytesUtil.toUShort(data)
    val optionScopeLength = data.getShort
    val optionLength = data.getShort
    val fieldCount = (optionLength + optionScopeLength) / 4
    NetFlowAnalysis.templates.get(tmpId) match {
      case Some(temp) => temp.updateTemplate(tmpId, fieldCount, data)
      case None =>
        val template = new SafeTemplate(tmpId, fieldCount)
        template.updateTemplate(tmpId, 0, data)
        NetFlowAnalysis.templates += (tmpId -> template)
    }
  }

  override def getTotalFlowSet(header: NetflowHeader): Int = header.fields(0).asInstanceOf[Int]

  override def getTemplate(tmpId: Int): Template = {
    NetFlowAnalysis.templates.getOrElse(tmpId,
      throw new RuntimeException("the template should no be null , tempID is " + tmpId))
  }

  // get the unix seconds from the header
  override def getUnixSeconds(header: NetflowHeader): Long = header.fields(2).asInstanceOf[Long]

}

/**
 * analysis the V5 netflow data
 */
class V5Analysis extends NetFlowAnalysis {

  override def getTemplate(tmpId: Int): Template = {
    NetFlowAnalysis.templates.getOrElse(0,
      throw new RuntimeException("the template should no be null , tempID is " + tmpId))
  }

  override def updateTemplate(data: ByteBuffer): Unit = {}

  override def isTemplateFlowSet(data: ByteBuffer): Boolean = false

  override def isTemplateExist(data: ByteBuffer): Int = 0

  // get the unix seconds from the header
  override def getUnixSeconds(header: NetflowHeader): Long = 0L

  override def getTotalFlowSet(header: NetflowHeader): Int = 0

  override def unPackHeader(data: ByteBuffer): NetflowHeader = { new NetflowHeader(null) }
}

