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
package cn.ac.ict.acs.netflow.load.worker.parquet

import java.nio.ByteBuffer

import cn.ac.ict.acs.netflow.load.utils.BytesUtil
import cn.ac.ict.acs.netflow.load.worker.Row
import cn.ac.ict.acs.netflow.load.worker.bgp.BGPRoutingTable
import cn.ac.ict.acs.netflow.load.worker.parser.Template

import scala.collection.immutable.HashMap
import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration

import parquet.hadoop.api.WriteSupport
import parquet.io.api.{ Binary, RecordConsumer }
import parquet.schema.PrimitiveType.PrimitiveTypeName._

import cn.ac.ict.acs.netflow.Logging

class RowWriteSupport extends WriteSupport[Row] with Logging {
  import ParquetSchema._

  private val schema = overallSchema
  private var writer: RecordConsumer = null

  override def init(configuration: Configuration) = {
    new WriteSupport.WriteContext(schema, new HashMap[String, String].asJava)
  }

  override def prepareForWrite(recordConsumer: RecordConsumer) = {
    writer = recordConsumer
  }

  override def write(record: Row) = {
    // Use netflowFields' ipv4_dst_addr or ipv6_dst_addr to search BGPRoutingTable
    val dstAddr = getDSTADDR(record.bb, record.startPos, record.template)
    val bgpTuple = BGPRoutingTable.search(dstAddr)

    writer.startMessage()

    // Write Header Fields
    writeSupplimentFields(FieldType.HEADER, record.header.fields)

    // Write Netflow Fields
    writeNetFlowFields(record.bb, record.startPos, record.template)

    // Write BGP Fields
    writeSupplimentFields(FieldType.BGP, bgpTuple.fields)

    writer.endMessage()
  }

  private def getDSTADDR(
    bb: ByteBuffer, start: Int, template: Template): (Array[Byte], Array[Byte]) = {

    var i = 0
    var curStart = start
    while (i < template.keys.length) {

      if (template.keys(i) == 12) {
        val ipv4_dst_addr = new Array[Byte](4)
        bb.get(ipv4_dst_addr, curStart, 4)
        return (ipv4_dst_addr, null)
      } else if (template.keys(i) == 28) {
        val ipv6_dst_addr = new Array[Byte](16)
        bb.get(ipv6_dst_addr, curStart, 16)
        return (null, ipv6_dst_addr)
      }

      i += 1
      curStart += template.values(i)
    }
    (null, null)
  }

  private def writeNetFlowFields(bb: ByteBuffer, start: Int, template: Template) = {
    var i = 0
    var curStart = start
    while (i < template.keys.length) {
      writeField(template.keys(i), bb, curStart, template.values(i))
      i += 1
      curStart += template.values(i)
    }
  }

  private def writeSupplimentFields(ft: FieldType.Value, fields: Array[Any]): Unit = {

    val length = ft match {
      case FieldType.HEADER => validHeader.length
      case FieldType.BGP => validBgp.length
    }

    var i = 0
    while (i < length) {

      if (fields(i) != null) {
        val (pos, tpe) = getPosAndType(ft, i)
        writer.startField(tpe.getName, pos)

        tpe.asPrimitiveType().getPrimitiveTypeName match {
          case INT64 =>
            writer.addLong(fields(i).asInstanceOf[Long])
          case BINARY =>
            writer.addBinary(Binary.fromString(fields(i).asInstanceOf[String]))
          case FIXED_LEN_BYTE_ARRAY =>
            writer.addBinary(Binary.fromByteArray(fields(i).asInstanceOf[Array[Byte]]))
        }

        writer.endField(tpe.getName, pos)
      }
      i += 1
    }
  }

  /**
   *
   * @param index initial index inside netflow fields
   * @param bb
   * @param start
   * @param length
   */
  private def writeField(index: Int, bb: ByteBuffer, start: Int, length: Int) = {
    val (pos, tpe) = getPosAndType(FieldType.NETFLOW, index)
    writer.startField(tpe.getName, pos)
    writeValue()
    writer.endField(tpe.getName, pos)

    def writeValue() = {
      tpe.asPrimitiveType.getPrimitiveTypeName match {
        case INT64 =>
          writer.addLong(BytesUtil.fieldAsLong(bb, start, length))
        case INT32 =>
          writer.addInteger(BytesUtil.fieldAsInt(bb, start, length))
        case BINARY | FIXED_LEN_BYTE_ARRAY =>
          val bytes = new Array[Byte](length)
          bb.get(bytes, start, length)
          writer.addBinary(Binary.fromByteArray(bytes))
      }
    }
  }
}
