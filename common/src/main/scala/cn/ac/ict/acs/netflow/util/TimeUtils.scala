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

  val SECOND_PATH_PATTERN = "/'year='yyyy/'month='MM/'day='dd/'hr='HH/'min='mm/'sec='ss"
  val MINUTE_PATH_PATTERN = "/'year='yyyy/'month='MM/'day='dd/'hr='HH/'min='mm"
  val HOUR_PATH_PATTERN = "/'year='yyyy/'month='MM/'day='dd/'hr='HH"
  val DAY_PATH_PATTERN = "/'year='yyyy/'month='MM/'day='dd"

  def newFormat(patternStr: String): DateTimeFormatter = {
    DateTimeFormat.forPattern(patternStr)
  }

  def createFormat: DateTimeFormatter = newFormat(CREATE_PATTERN)
  def showFormat: DateTimeFormatter = newFormat(SHOW_PATTERN)

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
          defaultUnit.convert(num, unit)
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
