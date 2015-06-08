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
import java.util.concurrent.ConcurrentHashMap


/**
 * store the netflow header's data
 * Created by ayscb on 2015/4/17.
 */
case class NetflowHeader(fields: Any*)

/**
 * Define the v9 template
 * @param tmpId  consist of routIP + tmpId
 * @param fieldsCount total fields number
 */
class SingleTemplate(val tmpId: String, val fieldsCount: Int)
  extends Iterable[(Int, Int)] {

  private var recordBytes = 0
  private val keyList = new Array[Int](fieldsCount)
  private val valueList = new Array[Int](fieldsCount)

  /**
   * update the template, for( v9 )
   * @param data template data
   */
  def updateTemplate(data: ByteBuffer):Unit = {

    for (i <- 0 until fieldsCount){
      val key = NetFlowShema.mapKey2Clm(data.getShort)
      if (key != -1) {
        val valueLen = data.getShort
        keyList(i) = key
        valueList(i) = valueLen
        recordBytes += valueLen
      }
    }
  }

  /**
   * create a single template for v5 v7
   * @param key_value
   */
  def createTemplate(key_value :(Int,Int)*): Unit ={
    assert(key_value.length == fieldsCount)
    var i=0
    key_value.foreach(x=>{
      keyList(i) = NetFlowShema.mapKey2Clm(x._1)
      valueList(i) = x._2
      i += 1
    })
  }

  override def iterator: Iterator[(Int, Int)] = new Iterator[(Int, Int)] {
    private var currId = 0
    override def hasNext: Boolean = currId < fieldsCount

    override def next(): (Int, Int) = {
      if (hasNext) {
        val nxt = (keyList(currId), valueList(currId))
        currId += 1
        nxt
      } else {
        throw new NoSuchElementException("next on empty iterator")
      }
    }
  }
}


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
 * resolve the V9 netflow data
 */

object V9Parser {
  private val version = 9

  private val templates = new ConcurrentHashMap[String,SingleTemplate](1024)

  private val tmp_key = new Array[Byte](18)   // store IP( ipv4 or ipv6) + flowsetID

  private def getkeyStr(key_array:Array[Byte]): String = key_array.mkString("")

  private def unPackHeader(data: ByteBuffer): NetflowHeader = {
    new NetflowHeader(
      //BytesUtil.toUShort(data),   // version  ( we have read this data )
      BytesUtil.toUShort(data),   // flowSetCount
      BytesUtil.toUInt(data),     // systemUptime
      BytesUtil.toUInt(data),     // unixSeconds
      BytesUtil.toUInt(data),     // packageSequence
      BytesUtil.toUInt(data)      // sourceID
      )
  }

  /** get total flw set from the header **/
  private def getTotalFlowSet(header: NetflowHeader): Int = header.fields.head.asInstanceOf[Int]

  /** get the unix seconds from the header **/
  private def getUnixSeconds(header: NetflowHeader): Long = header.fields(2).asInstanceOf[Long]

  private def isTemplateFlowSet(data: ByteBuffer): Boolean = {
    val flowSetID = data.getShort(data.position()) & 0xFFFF
    if (flowSetID < 256) true else false
  }

  private def getTemplate(tmp_key: Array[Byte], data: ByteBuffer): Option[SingleTemplate] = {

    val curPos = data.position()

    //TODO define the template ID ? netflow router ip + template ID
    tmp_key(16) = data.get(curPos)
    tmp_key(17) = data.get(curPos+1)
    val keyStr = getkeyStr(tmp_key)

    if (!templates.containsKey(keyStr)) {
      // Since the template does not exist , we do not understand the data.
      // So we should skip this flow set.
      // TODO skip the data ? or save the data
      val length = data.getShort(curPos+2) & 0xFFFF
      data.position(curPos + data.remaining().min(length))
      None
    } else {
      Some(templates.get(keyStr))
    }
  }

  private def updateTemplate(tmp_key: Array[Byte], data: ByteBuffer): Unit = {
    val flowSet = data.getShort
    val length = data.getShort - 4 // -2Byte (flowset) - 2Byte(length)

    flowSet match {
      case 1 => updateOptionTemplate(tmp_key, length, data)
      case x if x >= 0 => updateNormalTemplate(tmp_key, length, data)
      case _ =>
    }
  }

  /** start from template ID **/
  private def updateNormalTemplate(tmp_key: Array[Byte], length : Int, data: ByteBuffer): Unit = {

    val startPos = data.position()
    while ((data.position() - startPos) != length) {
     // data.get(tmp_key, 16, 18)     // get the template ID
      tmp_key(16) = data.get()
      tmp_key(17) = data.get()
      val fieldCount = data.getShort

      val keyStr = getkeyStr(tmp_key)
      val temp = new SingleTemplate(keyStr,fieldCount)
      temp.updateTemplate(data)
      templates.put(keyStr,temp)
    }
  }

  /** start from template ID **/
  private def updateOptionTemplate(tmp_key: Array[Byte], length : Int, data: ByteBuffer): Unit = {
    val startPos = data.position()

    // TODO: do we need to save the option template ? ( skip now )
    data.position(startPos + length)

//    val tmpId = BytesUtil.toUShort(data)
//    val optionScopeLength = data.getShort
//    val optionLength = data.getShort
//    val fieldCount = (optionLength + optionScopeLength) / 4
//    NetFlowAnalysis.templates.get(tmpId) match {
//      case Some(temp) => temp.updateTemplate(tmpId, fieldCount, data)
//      case None =>
//        val template = new SafeTemplate(tmpId, fieldCount)
//        template.updateTemplate(tmpId, 0, data)
//        NetFlowAnalysis.templates += (tmpId -> template)
//    }
  }


  def parseNetflow(parquetWriter: NetFlowWriterUtil, data: ByteBuffer) : Unit ={

      // copy the IP address, since a byteBuffer contain a single IP
    //  data.get(tmp_key,0,16)

      val headerData = unPackHeader(data)
      val totalFlowSet = getTotalFlowSet(headerData)
      val UnixSeconds = getUnixSeconds(headerData)

      var flowsetCount = 0

      if (totalFlowSet == 0) {
        //only contain template fowset
        // we only update the template and skip the package
        if (isTemplateFlowSet(data)){
          updateTemplate(tmp_key, data)
        }
      }else {
        // contain some data flowset
        while (flowsetCount != totalFlowSet && data.hasRemaining) {
          // the flow set is template , so we should undate the template
          if (isTemplateFlowSet(data)){
            updateTemplate(tmp_key, data)
          } else {
            // first we should check if the template exist ( v9 : >255, not exist : -1  )
            getTemplate(tmp_key, data) match {
              case Some(template) =>
                val record = new NetflowGroup(version, template, Array(UnixSeconds), data)
                parquetWriter.getNetflowWriter(UnixSeconds) match {
                  case Some(writer) => writer.write(record)
                  case _ =>
                }
                flowsetCount += record.flowCount
              case None =>
            }
          }
        }
      }
  }
}

/**
 * V5 format
 * ---------------------header-------------------------
 * |        version         |   flowSetCount (1-30)  |
 * |                    sysUpTime                    |
 * |                   Epoch seconds                 |
 * |                   Nano seconds                  |
 * |                   Flows Seen                    |
 * | EngineType | EngineID |      Sampling info      |
 * ----------------------body-------------------------
 * |                    flowSet                      |
 * |                    flowSet                      |
 * |                    ......                       |
 * ---------------------------------------------------
 *
 */
object V5Parser {

  private val version = 5
  private val template : SingleTemplate = {
    val tmp = new SingleTemplate("0",18)
    tmp.createTemplate(
      (35,1),(34,4),    // header
      (8,4),(12,4),(15,4),(10,2),(14,2),
      (2,4),(1,4),(22,4),(21,4),(7,2),
      (11,2),(6,1),(4,1),(5,1),(16,2),
      (17,2),(9,1),(13,1))
    tmp
  }

  private def unPackHeader(data: ByteBuffer): NetflowHeader = {
    new NetflowHeader(
      //BytesUtil.toUShort(data),   // version  ( we have read this data )
      BytesUtil.toUShort(data),     // flowSetCount
      BytesUtil.toUInt(data),       // system uptime
      BytesUtil.toUInt(data),       // EpochSeconds
      BytesUtil.toUInt(data),       // Nanoseconds
      BytesUtil.toUInt(data),       // Flows Seen
      BytesUtil.toUByte(data),      // Engine Type
      BytesUtil.toUByte(data),      // Engine ID
      BytesUtil.toUShort(data)      // Sampling info
    )}

  // get the unix seconds from the header
  private def getUnixSeconds(header: NetflowHeader): Long = header.fields(2).asInstanceOf[Long]

  private def getTotalFlowSet(header: NetflowHeader): Int = header.fields.head.asInstanceOf[Int]

  private def getSampling(header: NetflowHeader): (Byte,Int) = {
    val sampling = header.fields.last.asInstanceOf[Int]
    val mode = (sampling & 0x03).asInstanceOf[Byte]
    val interval = (sampling & 0x3FFF).asInstanceOf[Short]
    (mode,interval)
  }

  def parseNetflow(parquetWriter: NetFlowWriterUtil, data: ByteBuffer) : Unit ={

    val headerData = unPackHeader(data)
    val totalFlowSet = getTotalFlowSet(headerData)
    val UnixSeconds = getUnixSeconds(headerData)
    val sample = getSampling(headerData)

    var flowsetCount = 0

    while(flowsetCount != totalFlowSet){
      val record = new NetflowGroup(version, template, Array(UnixSeconds, sample._1, sample._2), data)
      parquetWriter.getNetflowWriter(UnixSeconds) match {
        case Some(writer) => writer.write(record)
        case _ =>
      }
      flowsetCount += 1
    }
  }
}

