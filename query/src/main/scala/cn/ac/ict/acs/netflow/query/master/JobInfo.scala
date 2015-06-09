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
package cn.ac.ict.acs.netflow.query.master

import scala.concurrent.duration.FiniteDuration

import org.joda.time.DateTime

import cn.ac.ict.acs.netflow.Query
import cn.ac.ict.acs.netflow.query.RestMessages.RestJobInfoResponse
import cn.ac.ict.acs.netflow.util.{ Utils, TimeUtils }

object JobType extends Enumeration {
  type JobType = Value

  val ADHOC, REPORT, ONLINE = Value
}

class JobInfo(
  val id: String,
  val desc: Option[String],
  val jobType: JobType.Value,
  val submitTime: DateTime,
  val deferTime: FiniteDuration,
  val frequency: Option[FiniteDuration],
  val query: Query,
  val jar: String,
  val mainClass: String,
  val appArgs: Array[String],
  val sparkProperties: Map[String, String],
  val environmentVariables: Map[String, String],
  val outputPath: String)
    extends Serializable {
  import JobState._

  def description = desc.getOrElse(mainClass)

  val fmt = TimeUtils.showFormat

  @transient var _state: JobState = RUNNABLE
  @transient var _submissionId: Option[String] = None
  @transient var _startTime: Option[DateTime] = None
  @transient var _endTime: Option[DateTime] = None
  @transient var _message: Option[String] = None
  @transient var _driverState: Option[String] = None

  def toRestJobInfoResponse: RestJobInfoResponse = {
    RestJobInfoResponse(id, desc, jobType.toString, submitTime.toString(fmt),
      deferTime.toString, frequency.map(_.toString), query, jar, mainClass, appArgs,
      sparkProperties, environmentVariables, outputPath,
      _state.toString, _submissionId,
      _startTime.map(_.toString(fmt)), _endTime.map(_.toString(fmt)), _message, _driverState)
  }

  private def readObject(in: java.io.ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
  }

  def schedule: Unit = _state = SCHEDULED

  def start(submitId: String): Unit = {
    _state = RUNNING
    _submissionId = Some(submitId)
    _startTime = Some(new DateTime)
  }

  def finish: Unit = {
    if (jobType == JobType.ADHOC || jobType == JobType.ONLINE) {
      _state = FINISHED
    } else {
      _state = RUNNABLE
    }
    _endTime = Some(new DateTime)
    _submissionId = None
  }

  def fail(message: Option[String] = None): Unit = {
    _state = FAILED
    _endTime = Some(new DateTime)
    _submissionId = None
    _message = message
  }

  def kill: Unit = {
    _state = KILLED
    _endTime = Some(new DateTime)
    _submissionId = None
  }

}
