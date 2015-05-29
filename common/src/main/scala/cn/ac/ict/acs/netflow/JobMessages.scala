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

import akka.actor.ActorRef

sealed trait JobMessages

object JobMessages {

  case class JobLaunched(jobId: String) extends JobMessages
  case class JobInitialize(
    tpe: String, // SQL or STREAM
    query: Query,
    outputPath: String,
    resultTracker: Option[ActorRef], // None for ONLINE and REPORT
    sparkMaster: String) extends JobMessages
  case object JobNotFound extends JobMessages
  case class WrongMaster(msg: String) extends JobMessages

  case class JobFinished(jobId: String) extends JobMessages
  case class JobFailed(jobId: String, err: Throwable) extends JobMessages
  case class JobKilled(jobId: String) extends JobMessages

  case object JobEndACK extends JobMessages

  case class JobResult(jobId: String, result: ResultDetail) extends JobMessages
  case class GetJobResult(jobId: String) extends JobMessages

  case class ResultDetail(schema: String, result: Array[String], totalLines: Long)
}
