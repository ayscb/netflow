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
import java.util

import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap

import org.apache.hadoop.conf.Configuration
import org.apache.spark.Logging
import parquet.hadoop.api.WriteSupport
import parquet.io.api.{Binary, RecordConsumer}
import parquet.schema.{MessageType, Type}
import parquet.schema.PrimitiveType.PrimitiveTypeName._

/**
 * get the parquet writer from some configure
 *
 */
// TODO
case class NetflowGroup(version : Int, template: SingleTemplate, Header: Array[AnyVal], data: ByteBuffer) {
  // netflow's statistic. For count the netflow numbers to judge when a record is over
  var flowCount = 0
  def getflowCount = flowCount
}

/**
 * get the writeSupport for analysis the record as one row ( nest format )
 * Created by ayscb on 2015/4/23.
 */
class DataFlowWriteSupport extends WriteSupport[NetflowGroup] with Logging {
  private val schema: MessageType = NetFlowShema.singleSchema
  private var write: RecordConsumer = null

  def getSchema = schema

  override def init(configuration: Configuration): WriteSupport.WriteContext = {
    log.debug(s"[ parquet : init the writesupport for the schema: $schema ]")
    new WriteSupport.WriteContext(schema, new HashMap[String, String].asJava)
  }

  override def prepareForWrite(recordConsumer: RecordConsumer): Unit = {
    log.debug(s"[ parquet : prepare for the write!!! ]")
    write = recordConsumer
  }

  override def write(record: NetflowGroup): Unit = {
    log.debug("[ parquet : the template' length is %d ] ".format(record.template.fieldsCount))
    log.debug("[ parquet : the data header' length is %d ] ".format(record.Header.length))

    record.version match {
      case 9 => parseVarFlowSetData(record)
      case _ => parseFixeFlowSetData(record)
    }
  }


  private def writePrimitiveValue(
    types: parquet.schema.PrimitiveType,
    data: ByteBuffer,
    dataLen: Int) = {

    types.getPrimitiveTypeName match {
      case INT64 =>
        val value = dataLen match {
          case 1 => BytesUtil.toUByte(data)
          case 2 => BytesUtil.toUShort(data)
          case 4 => BytesUtil.toUInt(data)
        }
        write.addLong(value)

      case INT32 =>
        val value = dataLen match {
          case 1 => BytesUtil.toUByte(data)
          case 2 => BytesUtil.toUShort(data)
        }
        write.addInteger(value)

      case BINARY =>
      case FIXED_LEN_BYTE_ARRAY =>
        write.addBinary(
          Binary.fromByteArray(
            util.Arrays.copyOfRange(data.array(), data.position(), data.position() + dataLen)))
        data.position(data.position() + dataLen)

      case INT96 => throw new RuntimeException("[ parquet ] No support this type")
      case _ => throw new RuntimeException("[ parquet ] No support this type")
    }
  }

  private def writeHeaderValue(
    types: parquet.schema.PrimitiveType,
    data: Any): Unit = {
    types.getPrimitiveTypeName match {
      case INT64 => write.addLong(data.asInstanceOf[Long])
      case INT32 => write.addInteger(data.asInstanceOf[Int])
      case BINARY =>
      case FIXED_LEN_BYTE_ARRAY =>
        val dataArray = data.asInstanceOf[Array[Byte]]
        write.addBinary(
          Binary.fromByteArray(util.Arrays.copyOf(dataArray, dataArray.length)))
      case INT96 => throw new RuntimeException("[ parquet ] No support this type")
      case _ => throw new RuntimeException("[ parquet ] No support this type")
    }
  }

  // the data ' position is from flowSet length
  private def parseVarFlowSetData(record: NetflowGroup): Unit = {

    // except 'flowSetID' and 'length' field length
    val startPos = record.data.position()
    val tempID = record.data.getShort
    val flowLen = record.data.getShort

    if(flowLen <= 0){
      //skip inaccurate package
      logWarning(s"[Netflow] The package's length should be > 0, but now $flowLen")
      return
    }

 //   val fields: util.List[Type] = schema.getFields // get whole fielsd
    var bytesCount = 4

    // we analysis the package as multirow
    while (bytesCount != flowLen) {

      if(bytesCount > flowLen ){
        // skip inaccurate package
        logWarning(s"[Netflow] The package's length should be $flowLen, but now $bytesCount")
        record.data.position(startPos+flowLen)
        return
      }

      var clmIdx = 0
      write.startMessage()

      // write the header
      record.Header.foreach(x => {
 //       val curFields = fields.get(clmIdx).asPrimitiveType()
        val curFields = schema.getType(clmIdx).asPrimitiveType()
        write.startField(curFields.getName, clmIdx)
        writeHeaderValue(curFields, x)
        write.endField(curFields.getName, clmIdx)
        clmIdx += 1
      })

      // write the body
      val temp = record.template.iterator
      temp.foreach(field => {
        val curClmId = clmIdx + field._1
    //    val curFields = fields.get(curClmId).asPrimitiveType()
        val curFields = schema.getType(curClmId).asPrimitiveType()

        write.startField(curFields.getName, curClmId)
        writePrimitiveValue(curFields, record.data, field._2)
        write.endField(curFields.getName, curClmId)
        bytesCount += field._2
      })

      write.endMessage()
      record.flowCount +=  1
    }
  }

  private def parseFixeFlowSetData(record:NetflowGroup): Unit = {

      val fields: util.List[Type] = schema.getFields // get whole fielsd

      var clmIdx = 0

      write.startMessage()
      // write the header
      record.Header.foreach(x => {
        val curFields = fields.get(clmIdx).asPrimitiveType()
        write.startField(curFields.getName, clmIdx)
        writeHeaderValue(curFields, x)
        write.endField(curFields.getName, clmIdx)
        clmIdx += 1
      })

      // write the body
      val tempIter = record.template.iterator
      tempIter.foreach(field => {
        val curClmId = clmIdx + field._1
        val curFields = fields.get(curClmId).asPrimitiveType()
        write.startField(curFields.getName, curClmId)
        writePrimitiveValue(curFields, record.data, field._2)
        write.endField(curFields.getName, field._1)
      })
      write.endMessage()
      record.flowCount =  1
    }
}
