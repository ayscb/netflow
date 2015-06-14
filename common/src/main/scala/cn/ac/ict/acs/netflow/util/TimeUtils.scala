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
package cn.ac.ict.acs.netflow.util

import java.util.concurrent.TimeUnit

import org.joda.time.DateTime
import org.joda.time.format.{ DateTimeFormatter, DateTimeFormat }

import cn.ac.ict.acs.netflow.NetFlowConf

object TimeUtils {

  val CREATE_PATTERN = "yyyyMMddHHmmss"
  val SHOW_PATTERN = "yyyy-MM-dd,HH:mm:ss"

  val SECOND_PATH_PATTERN = "/yyyy/MM/dd/HH/mm/ss"
  val MINUTE_PATH_PATTERN = "/yyyy/MM/dd/HH/mm"
  val HOUR_PATH_PATTERN = "/yyyy/MM/dd/HH"
  val DAY_PATH_PATTERN = "/yyyy/MM/dd"

  def newFormat(patternStr: String): DateTimeFormatter = {
    DateTimeFormat.forPattern(patternStr)
  }

  def createFormat: DateTimeFormatter = newFormat(CREATE_PATTERN)
  def showFormat: DateTimeFormatter = newFormat(SHOW_PATTERN)

  val LOAD_DIR_CREATION_INTERVAL = "netflow.load.dir.creation.interval"

  def loadDirFormat(conf: NetFlowConf): DateTimeFormatter = {
    val interval = loadDirIntervalSec(conf)

    val fmtStr = if (interval < 60) { // less than 1 min
      SECOND_PATH_PATTERN
    } else if (interval < 60 * 60) { // less than 1 hour
      MINUTE_PATH_PATTERN
    } else if (interval < 60 * 60 * 24) { // less than 1 day
      HOUR_PATH_PATTERN
    } else {
      DAY_PATH_PATTERN
    }
    newFormat(fmtStr)
  }

  def loadDirIntervalSec(conf: NetFlowConf): Long = {
    val interval = conf.get(LOAD_DIR_CREATION_INTERVAL, "10min")
    timeStringAsSec(interval)
  }

  /**
   * get the file path as "2015/02/21/03/23"
   * @param conf
   * @param seconds since epoch
   * @return
   */
  def getTimeBasePathBySeconds(seconds: Long, conf: NetFlowConf): String = {
    val pathFmt = loadDirFormat(conf)
    new DateTime(seconds * 1000).toString(pathFmt)
  }

  /**
   * get next interval time.
   * Suppose the dictionary interval time is 10 min ,
   * when the time is  1:12 , the method will return 1:00
   * @param conf
   * @param second
   * @return
   */
  def getPreviousBaseTime(second: Long, conf: NetFlowConf): Long = {
    val interval = loadDirIntervalSec(conf)
    second / interval * interval - interval
  }

  /**
   * get current interval time .
   * if the dictionary interval time is 10 min ,
   * if the time is  1:12 , the method will return 1:10
   * @param conf
   * @param second
   * @return
   */
  def getCurrentBastTime(second: Long, conf: NetFlowConf): Long = {
    val interval = loadDirIntervalSec(conf)
    second / interval * interval
  }

  /**
   * get next interval time .
   * if the dictionary interval time is 10 min ,
   * if the time is  1:12 , the method will return 1:20
   * @param conf
   * @param second
   * @return
   */
  def getNextBaseTime(second: Long, conf: NetFlowConf): Long = {
    val interval = loadDirIntervalSec(conf)
    second / interval * interval + interval
  }

  /**
   * Convert str as seconds
   * @param str to convert
   * @return seconds
   */
  def timeStringAsSec(str: String): Long = {
    parseTimeString(str, TimeUnit.SECONDS)
  }

  /**
   * Convert str as MilliSeconds
   * @param str to convert
   * @return millis
   */
  def timeStringAsMs(str: String): Long = {
    parseTimeString(str, TimeUnit.MILLISECONDS)
  }

  /**
   * Convert a passed time string (e.g. 50s, 100ms, or 250us) to a time count for
   * internal use. If no suffix is provided a direct conversion is attempted.
   */
  def parseTimeString(str: String, defaultUnit: TimeUnit): Long = {
    val lower = str.toLowerCase.trim

    try {
      val regex = "(-?[0-9]+)([a-z]+)?".r

      lower match {
        case regex(n, u) => {
          val num = n.toLong
          val unit = if (u != null) {
            if (TIME_SUFFIXES.contains(u)) {
              TIME_SUFFIXES.get(u).get
            } else {
              throw new NumberFormatException(s"Invalid suffix: $u")
            }
          } else {
            defaultUnit
          }
          return defaultUnit.convert(num, unit)
        }
        case _ =>
          throw new NumberFormatException("Failed to parse time string: " + str)
      }
    } catch {
      case e: NumberFormatException =>
        val timeError: String = "Time must be specified as seconds (s), " +
          "milliseconds (ms), microseconds (us), minutes (m or min), hour (h), or day (d). " +
          "E.g. 50s, 100ms, or 250us."
        throw new NumberFormatException(timeError + "\n" + e.getMessage)
    }
  }

  val TIME_SUFFIXES = Map(
    "us" -> TimeUnit.MICROSECONDS,
    "ms" -> TimeUnit.MILLISECONDS,
    "s" -> TimeUnit.SECONDS,
    "m" -> TimeUnit.MINUTES,
    "min" -> TimeUnit.MINUTES,
    "h" -> TimeUnit.HOURS,
    "d" -> TimeUnit.DAYS)

}
