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
import org.joda.time.format.DateTimeFormatter

import cn.ac.ict.acs.netflow.util.TimeUtils

package object load {
  import TimeUtils._
  import LoadConf._

  val systemBasePath = "/netflow"

  private def loadDirFormat(conf: NetFlowConf): DateTimeFormatter = {
    val interval = dirCreationInterval(conf)

    val fmtStr = if (interval < 1000 * 60) { // less than 1 min
      SECOND_PATH_PATTERN
    } else if (interval < 1000 * 60 * 60) { // less than 1 hour
      MINUTE_PATH_PATTERN
    } else if (interval < 1000 * 60 * 60 * 24) { // less than 1 day
      HOUR_PATH_PATTERN
    } else {
      DAY_PATH_PATTERN
    }
    newFormat(fmtStr)
  }

  /**
   *
   * @param conf netflow configure
   * @return  the ms time
   */
  def dirCreationInterval(conf: NetFlowConf): Long = {
    val interval = conf.get(LOAD_DIR_CREATION_INTERVAL, "10min")
    timeStringAsMs(interval)
  }

  /**
   *
   * @param conf  netflow configure
   * @return  ms Time
   */
  def writerCloseDelay(conf: NetFlowConf): Long = {
    val interval = conf.get(CLOSE_DELAY, "3min")
    timeStringAsMs(interval)
  }

  /**
   * get the file path as "2015/02/21/03/23"
   * @param millis since epoch
   * @param conf
   * @return
   */
  def getPathByTime(millis: Long, conf: NetFlowConf): String = {
    val pathFmt = loadDirFormat(conf)
    systemBasePath.concat(new DateTime(millis).toString(pathFmt))
  }

  /**
   * get timebase .
   * if the dictionary interval time is 10 min ,
   * if the time is  1:12 , the method will return 1:10
   * @param conf
   * @param millis
   * @return
   */
  def getTimeBase(millis: Long, conf: NetFlowConf): Long = {
    val interval = dirCreationInterval(conf)
    millis / interval * interval
  }

  /**
   * get the remaining time from 'current time 'to 'base time + delay time + interval time'
   * @param curMillis  current millis time
   * @param intervalMs  interval time ms
   * @param delay   delay time ms
   * @param conf
   * @return
   */
  def getRemainTimes(curMillis: Long, intervalMs: Long, delay: Long, conf: NetFlowConf): Long = {
    val baseTime = getTimeBase(curMillis, conf)
    intervalMs + delay - (curMillis - baseTime)
  }
}

