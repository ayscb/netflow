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
package cn.ac.ict.acs.netflow.load.utils

import java.nio.{ByteOrder, ByteBuffer}

object BytesUtil {

  def toUShort(value: ByteBuffer): Int =
    value.getShort & 0xFFFF

  def toUByte(value: ByteBuffer): Int =
    value.get() & 0xFF

  def toUInt(value: ByteBuffer): Long = {
    if (value.remaining() > 4) {
      val v = value.getInt
      v & 0xFFFFFFFFL
    } else {
      println(value.array().take(20).map(x => x & 0xff).mkString(";"))
      0L
    }
  }

  def fieldAsLong(bb: ByteBuffer, start: Int, length: Int): Long = {
    if (bb.order == ByteOrder.BIG_ENDIAN) {
      fieldAsBELong(bb, start, length)
    } else {
      fieldAsLELong(bb, start, length)
    }
  }

  def fieldAsInt(bb: ByteBuffer, start: Int, length: Int): Int = {
    if (bb.order == ByteOrder.BIG_ENDIAN) {
      fieldAsBEInt(bb, start, length)
    } else {
      fieldAsLEInt(bb, start, length)
    }
  }

  // BIG ENDIAN
  // Regard `length` bytes as a long value
  def fieldAsBELong(bb: ByteBuffer, start: Int, length: Int): Long = {
    var i = 0
    var result: Long = 0
    while (i < length) {
      result = bb.get(start + i) + result << 8
      i += 1
    }
    result
  }

  // LITTLE ENDIAN
  def fieldAsLELong(bb: ByteBuffer, start: Int, length: Int): Long = {
    var i = 0
    var result: Long = 0
    while (i < length) {
      result += bb.get(start + i) << (8 * i)
      i += 1
    }
    result
  }

  // BIG ENDIAN
  def fieldAsBEInt(bb: ByteBuffer, start: Int, length: Int): Int = {
    var i = 0
    var result: Int = 0
    while (i < length) {
      result = bb.get(start + i) + result << 8
      i += 1
    }
    result
  }

  // LITTLE ENDIAN
  def fieldAsLEInt(bb: ByteBuffer, start: Int, length: Int): Int = {
    var i = 0
    var result: Int = 0
    while (i < length) {
      result = bb.get(start + i) << (8 * i)
      i += 1
    }
    result
  }

}
