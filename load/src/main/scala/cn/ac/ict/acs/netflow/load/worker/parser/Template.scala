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
import java.util.concurrent.ConcurrentHashMap

case class TemplateKey(routerIp: Array[Byte], templateId: Int) {
  override def hashCode(): Int = {
    java.util.Arrays.hashCode(routerIp) + templateId
  }

  override def equals(obj: scala.Any): Boolean = {
    if (obj == null) return false
    //    if(this == obj) return true
    if (getClass != obj.getClass) return false
    val oj = obj.asInstanceOf[TemplateKey]
    if (oj.templateId != templateId) return false
    java.util.Arrays.equals(this.routerIp, oj.routerIp)
  }
}

class Template(val tmpId: Int, val fieldsCount: Int) {

  var rowLength = 0
  val keys = new Array[Int](fieldsCount)
  val values = new Array[Int](fieldsCount)

  def this(tmpId: Int, fieldsCount: Int, data: ByteBuffer, curPos: Int) {
    this(tmpId, fieldsCount)
    createTemplate(data, curPos)
  }

  def this(tmpId: Int, fieldsCount: Int, key_value: Seq[(Int, Int)]) {
    this(tmpId, fieldsCount)
    createTemplate(key_value)
  }

  /**
   * create a template when this flowset is a template flowset,
   * only for v9
   * @param data template data
   */
  def createTemplate(data: ByteBuffer, curPos: Int): Template = {

    var i = 0
    var pos = curPos
    while (i != fieldsCount) {
      val key = data.getShort(pos); pos += 2
      val valueLen = data.getShort(pos); pos += 2
      keys(i) = key
      values(i) = valueLen
      rowLength += valueLen
      i += 1
    }
    this
  }

  /**
   * create a single template for v5 v7
   * @param key_value
   */
  def createTemplate(key_value: Seq[(Int, Int)]): Template = {
    assert(key_value.length == fieldsCount)
    var i = 0
    key_value.foreach(x => {
      keys(i) = x._1
      values(i) = x._2
      i += 1
    })
    this
  }
}
