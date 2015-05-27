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

import cn.ac.ict.acs.netflow.JobMessages.ResultDetail
import cn.ac.ict.acs.netflow.broker.ValidJobDescription

sealed trait RestMessage

object RestMessages {

  /**
   * GET /status
   */
  case object RestQueryMasterStatusRequest extends RestMessage

  case class RestQueryMasterStatusResponse(
      NetFlowVersion: String,
      APIVersion: String = "v1",
      AliveBrokers: Seq[String],
      ExistJobs: Seq[String] // TODO more info to return?
    ) extends RestMessage

  /**
   * GET /netflow/v1/jobs
   */
  case object RestAllJobsInfoRequest extends RestMessage

  case class RestAllJobsInfoResponse(jobs: Seq[JobInfoAbstraction]) extends RestMessage

  case class JobInfoAbstraction(
      jobId: String,
      submitTime: String,
      sql: String,
      state: String,
      submissionId: Option[String],
      startTime: Option[String],
      endTime: Option[String],
      message: Option[String],
      driverState: Option[String]
    )

  case class RestJobStatusRequest(jobId: String) extends RestMessage

  case class RestJobStatusResponse(
      jobId: String,
      state: String,
      submitTime: String,
      startTime: Option[String],
      endTime: Option[String],
      result: Option[ResultDetail],
      message: Option[String]
    ) extends RestMessage

  /**
   * GET /netflow/v1/job/<jobId>
   *
   */
  case class RestJobInfoRequest(jobId: String) extends RestMessage

  case class RestJobInfoResponse(
      jobId: String,
      desc: Option[String],
      jobType: String,
      submitTime: String,
      deferTime: String,
      frequency: Option[String],
      query: Query,
      jar: String,
      mainClass: String,
      appArgs: Array[String],
      sparkProperties: Map[String, String],
      environmentVariables: Map[String, String],
      outputPath: String,
      state: String,
      submissionId: Option[String],
      startTime: Option[String],
      endTime: Option[String],
      message: Option[String],
      driverState: Option[String]
    ) extends RestMessage

  /**
   * POST /netflow/v1/jobs
   *
   */
  case class RestSubmitJobRequest(jobDesc: ValidJobDescription) extends RestMessage

  case class RestSubmitJobResponse(
      jobId: String,
      success: Boolean,
      message: Option[String]) extends RestMessage

  /**
   * DELETE /netflow/v1/jobs/<jobId>
   *
   */
  case class RestKillJobRequest(jobId: String) extends RestMessage

  case class RestKillJobResponse(
      jobId: String,
      success: Boolean,
      message: Option[String]) extends RestMessage

  // message of failure whatever the request was
  case class RestFailureResponse(message: String) extends RestMessage
}
