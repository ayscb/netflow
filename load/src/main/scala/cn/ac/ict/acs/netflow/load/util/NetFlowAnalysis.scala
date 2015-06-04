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

import scala.collection.mutable

/**
 * store the netflow header's data
 * Created by ayscb on 2015/4/17.
 */
case class NetflowHeader(fields: Any*)

/**
 * Define the v9 template
 * @param tmpId
 * @param fieldsCount
 */
class SingleTemplate(val tmpId: String, val fieldsCount: Int)
  extends Iterable[(Int, Int)] {

  private var recordBytes = 0
  private val keyList = new Array[Int](fieldsCount)
  private val valueList = new Array[Int](fieldsCount)

  /**
   * update the template,
   * @param data template data
   */
  def updateTemplate(data: ByteBuffer):Unit = {

    for (i <- 0 until fieldsCount){
      val key = NetFlowShema.mapKey2Clm(BytesUtil.toUShort(data))
      if (key != -1) {
        val valueLen = BytesUtil.toUShort(data)
        keyList(i) = key
        valueList(i) = valueLen
        recordBytes += valueLen
      }
    }
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

object NetFlowAnalysis {
  // for all netflow version( V5, V7, V8 ,V9 ) shared
 // val templates = new mutable.HashMap[Int, Template]
  val templates = new ConcurrentHashMap[Int,SingleTemplate](1024)
}


abstract class NetFlowAnalysis {

  def isTemplateFlowSet(data: ByteBuffer): Boolean
  def updateTemplate(data: ByteBuffer): Unit

  /**
   * check whether the template exist.
   * Since v9 needs template to analysis the data , so we should get the template first
   * for other version , the template has already existed when we new a object
   * @param data
   * @return
   *         not exist : -1
   *         v9        : >255
   *         others    : 0
   */
  def isTemplateExist(data: ByteBuffer): Int

  def unPackHeader(data: ByteBuffer): NetflowHeader

  def getTemplate(tmpId: Int): Template

  // get the unix seconds from the header
  def getUnixSeconds(header: NetflowHeader): Long

  def getTotalFlowSet(header: NetflowHeader): Int

}
