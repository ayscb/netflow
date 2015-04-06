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
package cn.ac.ict.acs.netflow.load

import java.net.InetAddress
import java.util.Calendar

class FilePath(HDFS: String, rootPath: String, intervalSecond: Int) {
  val cal: Calendar = Calendar.getInstance()
  val hostname = InetAddress.getLocalHost.getHostName
  val foot = ".par"


  //  def getFilePathByMillMs( utc : Long) : String = {
  //    cal.setTimeInMillis( utc * 1000)
  //    val sb = new StringBuilder(HDFS).append("/")
  //      .append(this.rootPath).append("/")
  //    val y = cal.get(Calendar.YEAR)
  //    val m = cal.get(Calendar.MONTH + 1)
  //    val d = cal.get(Calendar.DAY_OF_MONTH)
  //    val h = cal.get(Calendar.HOUR_OF_DAY)
  //    val M = cal.get(Calendar.MINUTE)
  //
  //    val _M = (M  / (this.intervalSecond / 60) ) * (this.intervalSecond / 60)
  //
  //    sb.append(y).append("/")
  //      .append(cp(m)).append("/")
  //      .append(cp(d)).append("/")
  //      .append(cp(h)).append("/")
  //      .append(cp(_M)).append("/")
  //      .append(this.hostname).append(System.currentTimeMillis()).append(foot)
  //    sb.toString()
  //  }

  def getFilePathByMillMs(utc: Long): String = {
    cal.setTimeInMillis(utc * 1000)
    val sb = new StringBuilder(HDFS).append("/")
      .append(this.rootPath).append(foot).append("/")
    sb.toString()
  }

  private def cp(value: Int): String = {
    if (value < 10) {
      "0".concat(value.toString)
    } else {
      value.toString
    }
  }
}

