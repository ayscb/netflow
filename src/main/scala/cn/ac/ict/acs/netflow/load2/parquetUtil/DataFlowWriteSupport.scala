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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.ac.ict.acs.netflow.load2.parquetUtil

import java.nio.ByteBuffer
import java.util

import cn.ac.ict.acs.netflow.load2.NetFlowShema
import cn.ac.ict.acs.netflow.load2.netFlow.BytesUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.Logging
import parquet.hadoop.api.WriteSupport
import parquet.io.api.{Binary, RecordConsumer}
import parquet.schema.{Type, MessageType}

import scala.collection.immutable.HashMap
import scala.collection.JavaConverters._

/**
 * get the writeSupport for analysis the record as one row ( nest format )
 * Created by ayscb on 2015/4/23.
 */

abstract class DataFlowWriteSupport extends WriteSupport[NetflowGroup] with Logging {
  protected var schema: MessageType = _
  protected var write: RecordConsumer = null

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
    log.debug("[ parquet : the data header' length is %d ] ".format(record.Header.size))
    analysisFlowSetData(record)
  }

  // the data ' position is from flowSet length
  protected def analysisFlowSetData(record: NetflowGroup): Unit

  import parquet.schema.PrimitiveType.PrimitiveTypeName._

  protected def writePrimitiveValue(
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
      case FIXED_LEN_BYTE_ARRAY => write.addBinary(
        Binary.fromByteArray(util.Arrays.copyOfRange(data.array(), data.position(), data.position() + dataLen))
      )
        data.position(data.position() + dataLen)

      case INT96 => throw new RuntimeException("[ parquet ] No support this type")
      case _ => throw new RuntimeException("[ parquet ] No support this type")
    }
  }

  protected def writeHeaderValue(
                                types: parquet.schema.PrimitiveType,
                                data : Any): Unit ={
    types.getPrimitiveTypeName match {
      case INT64 => write.addLong(data.asInstanceOf[Long])
      case INT32 => write.addInteger(data.asInstanceOf[Int])
      case BINARY =>
      case FIXED_LEN_BYTE_ARRAY =>
        val dataArray  = data.asInstanceOf[Array[Byte]]
        write.addBinary(
          Binary.fromByteArray(util.Arrays.copyOf(dataArray, dataArray.size))
        )
      case INT96 => throw new RuntimeException("[ parquet ] No support this type")
      case _ => throw new RuntimeException("[ parquet ] No support this type")
    }
  }

}

class DataFlowSingleWriteSupport extends DataFlowWriteSupport {
  schema = NetFlowShema.singleSchema

  override  def analysisFlowSetData(record: NetflowGroup): Unit = {

    // except 'flowSetID' and 'length' field length
    val tempID = record.data.getShort
    val flowLen = BytesUtil.toUShort(record.data) - 4

    val fields: util.List[Type]  = schema.getFields    // get whole fielsd
    var bytesCount = 0

    // we analysis the package as multirow
    while( bytesCount != flowLen ){
      var clmIdx = 0
      write.startMessage()

      // write the header
      record.Header.foreach( x => {
        val curFields = fields.get(clmIdx).asPrimitiveType()
        write.startField( curFields.getName, clmIdx )
        writeHeaderValue(curFields, x )
        write.endField( curFields.getName,clmIdx)
        clmIdx += 1
      })

      // write the body
      val temp = record.template.iterator
      temp.foreach( field => {
        val curClmId = clmIdx + field._1
        val curFields = fields.get( curClmId ).asPrimitiveType()
        write.startField(curFields.getName , curClmId )
        writePrimitiveValue(curFields, record.data, field._2)
        write.endField(curFields.getName, field._1)
        bytesCount += field._2
      })

      write.endMessage()
    //  bytesCount += record.template.getRecordBytes
      record.flowCount = record.flowCount + 1
    }
  }
}

class DataFlowMultiWriteSupport extends DataFlowWriteSupport {
  schema = NetFlowShema.singleSchema

  override def analysisFlowSetData(record: NetflowGroup): Unit = {

    // except 'flowSetID' and 'length' field length
    val tempID = record.data.getShort
    val flowLen = BytesUtil.toUShort(record.data) - 4
    write.startMessage()

    val wholeFields = schema.getFields

    var clmId = 0
    while( wholeFields.get(clmId).isPrimitive ){
      if( record.Header(clmId) != null ){
        val curField = wholeFields.get(clmId).asPrimitiveType()
        write.startField(curField.getName , clmId )
        writeHeaderValue(curField,record.Header(clmId))
        write.startField(curField.getName , clmId )
      }
      clmId += 1
    }

    val fields: util.List[Type] = schema.getFields.get(1).asGroupType().getFields

    var flowsetBytesCount = 0
    write.startField(schema.getFields.get(1).getName,1)
    while (flowsetBytesCount != flowLen) {
      write.startGroup()
      record.template.iterator
        .foreach( fieldTuple => {
          val primitiveField = fields.get(fieldTuple._1).asPrimitiveType()
          write.startField(primitiveField.getName, fieldTuple._1)
          writePrimitiveValue(primitiveField, record.data, fieldTuple._2)
          write.endField(primitiveField.getName, fieldTuple._1)
          flowsetBytesCount += fieldTuple._2
      })
      write.endGroup()
    //  flowsetBytesCount += record.template.getRecordBytes //field 2B + length 2
      record.flowCount = record.flowCount + 1
    }
    write.endField(schema.getFields.get(1).getName,1)
    write.endMessage()
  }
}