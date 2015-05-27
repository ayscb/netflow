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

import org.joda.time.DateTime

import cn.ac.ict.acs.netflow.NetFlowConf

object TimeUtil {

  /**
   * get the file path as "2015/02/21/03/23"
   * @param conf
   * @param seconds
   * @return
   */
  def getTimeBasePathBySeconds(conf: NetFlowConf, seconds :Long) : String = {
    val str = conf.doctTimeIntervalFormat
    new DateTime(seconds * 1000).toString(str)
  }

  /**
   * get next interval time.
   * Support the dictionary interval time is 10 min ,
   * when the time is  1:12 , the method will return 1:00
   * @param conf
   * @param second
   * @return
   */
  def getPreviousBaseTime( conf:NetFlowConf, second : Long ):Long ={
    val value = conf.doctTimeIntervalValue.toLong
    second / value * value - value
  }

  /**
   * get current interval time .
   * if the dictionary interval time is 10 min ,
   * if the time is  1:12 , the method will return 1:10
   * @param conf
   * @param second
   * @return
   */
  def getCurrentBastTime( conf:NetFlowConf, second : Long) : Long ={
    val value = conf.doctTimeIntervalValue.toLong
    second / value * value
  }

  /**
   * get next interval time .
   * if the dictionary interval time is 10 min ,
   * if the time is  1:12 , the method will return 1:20
   * @param conf
   * @param second
   * @return
   */
  def getNextBaseTime( conf:NetFlowConf, second : Long) : Long ={
    val value = conf.doctTimeIntervalValue.toLong
    second / value * value + value
  }

  def timeToSeconds(conf: NetFlowConf, t: String) =
    DateTime.parse(t, conf.timeFormat).getMillis / 1000

  def secnodsToTime(conf: NetFlowConf, seconds :Long )  =
    new DateTime(seconds * 1000).toString(conf.timeFormat)

}
