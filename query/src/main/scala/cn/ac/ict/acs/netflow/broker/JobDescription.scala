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
package cn.ac.ict.acs.netflow.broker

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

import cn.ac.ict.acs.netflow.{Query, NetFlowException}
import cn.ac.ict.acs.netflow.master.JobType


case class JobDescription(
    description: Option[String],
    jobType: Option[String],
    deferTime: Option[RelativeTime],
    frequency: Option[RelativeTime],
    jobQuery: Option[Query],
    jar: Option[String],
    mainClass: Option[String],
    appArgs: Option[Array[String]],
    sparkProperties: Option[Map[String, String]],
    environmentVariables: Option[Map[String, String]],
    outputPath: Option[String]) {

  /**
   *
   * @return (validJobDesc, success, message)
   */
  def doValidate(): (ValidJobDescription, Boolean, Option[String]) = {
    val tpe = try {
      if (jobType.isDefined) {
        JobType.withName(jobType.get.toUpperCase)
      } else {
        return (null, false, Some(s"No jobType provided"))
      }
    } catch {
      case e: NoSuchElementException =>
        return (null, false, Some(s"Job Type: ${jobType.get} is not a valid value"))
    }

    val defer = try {
      deferTime.map(_.toDuration).getOrElse(0.days)
    } catch {
      case e: NetFlowException =>
        return (null, false, Some(e.getMessage))
    }

    val freq = try {
      frequency.map(_.toDuration)
    } catch {
      case e: NetFlowException =>
        return (null, false, Some(e.getMessage))
    }

    if (tpe == JobType.REPORT && !frequency.isDefined) {
      return (null, false, Some(s"Report Job must specify running frequency"))
    }

    val query = try {
      require(jobQuery.isDefined, "No valid jobQuery available")
      jobQuery.get
    } catch {
      case e: IllegalArgumentException =>
        return (null, false, Some(e.getMessage))
    }

    val vjd = ValidJobDescription(description, tpe, defer, freq, query,
      jar, mainClass, appArgs, sparkProperties, environmentVariables, outputPath)

    (vjd, true, None)
  }
}

case class RelativeTime(num: Int, unit: String) {
  def toDuration: FiniteDuration = {
    unit match {
      case "day" | "days" => new FiniteDuration(num, TimeUnit.DAYS)
      case "hour" | "hours"=> new FiniteDuration(num, TimeUnit.HOURS)
      case "minute" | "minutes" => new FiniteDuration(num, TimeUnit.MINUTES)
      case "second" | "seconds" => new FiniteDuration(num, TimeUnit.SECONDS)
      case "millisecond" | "milliseconds" => new FiniteDuration(num, TimeUnit.MILLISECONDS)
      case x: String =>
        throw new NetFlowException(s"Unsupported time unit: $x")
    }
  }
}

case class ValidJobDescription(
    description: Option[String],
    jobType: JobType.Value,
    deferTime: FiniteDuration,
    frequency: Option[FiniteDuration],
    query: Query,
    jar: Option[String],
    mainClass: Option[String],
    appArgs: Option[Array[String]],
    sparkProperties: Option[Map[String, String]],
    environmentVariables: Option[Map[String, String]],
    outputPath: Option[String])

