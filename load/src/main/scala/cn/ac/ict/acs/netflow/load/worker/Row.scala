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
package cn.ac.ict.acs.netflow.load.worker

import java.nio.ByteBuffer

import cn.ac.ict.acs.netflow.load.worker.parser.Template

// time: Long, routerIpv4: Array[Byte], routerIpv6: Array[Byte]
class RowHeader(val fields: Array[Any])

abstract class Row {
  def header: RowHeader
  def bb: ByteBuffer
  def startPos: Int
  def template: Template
  final def length: Int = {
    require(template != null)
    template.rowLength
  }
}

class MutableRow(val bb: ByteBuffer, val template: Template) extends Row {
  var header: RowHeader = _
  var startPos: Int = _

  def setHeader(rowheader: RowHeader): Unit = {
    header = rowheader
  }

  /**
   * a new row appears
   * @param start
   */
  def update(start: Int): Unit = {
    this.startPos = start
  }
}

