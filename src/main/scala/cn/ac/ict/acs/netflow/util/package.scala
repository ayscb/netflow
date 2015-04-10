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
package cn.ac.ict.acs.netflow

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

package object util {

}

object TimeUtil {
  def timeToSeconds(conf: NetFlowConf, t: String) =
    DateTime.parse(t, conf.timeFormat).getMillis / 1000

  def secnodsToTime(conf: NetFlowConf, seconds :Long )  =
    new DateTime(seconds * 1000).toString(conf.timeFormat)

  def secnodsToTime(seconds :Long )  =
    new DateTime(seconds * 1000).toString(
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm"))
}

trait IP {
  def str2Bytes(ip: String): Array[Byte]
  def bytes2String(ip: Array[Byte]): String

  final def toInt(b: Byte): Int = b & 0xFF
}

object IPv4 extends IP {

  def str2Bytes(ip: String) = ip.split('.').map(_.toInt.toByte)

  def bytes2String(ip: Array[Byte]) = ip.map(toInt).mkString(".")
}

object IPv6 extends IP {
  def str2Bytes(ip: String) = ???

  def bytes2String(ip: Array[Byte]) = ???
}
