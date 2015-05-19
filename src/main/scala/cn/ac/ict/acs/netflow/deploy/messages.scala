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
package cn.ac.ict.acs.netflow.deploy

import cn.ac.ict.acs.netflow.deploy.qmaster.JobState._
import cn.ac.ict.acs.netflow.deploy.qmaster.JobType


sealed trait RestMessage

object RestMessages {

  /**
   * GET /status
   */
  case object RestRequestQueryMasterStatus extends RestMessage

  case class RestResponseQueryMasterStatus(
                                            version: String,
                                            runningJobs: Seq[String],
                                            finishedJobs: Seq[String] //TODO more info to return?
                                            ) extends RestMessage

  /**
   * GET /netflow/v1/jobs
   */
  case object RestRequestAllJobsInfo extends RestMessage

  case class RestResponseAllJobsInfo(jobs: Seq[RestResponseJobInfo]) extends RestMessage

  /**
   * GET /netflow/v1/job/<jobId>
   *
   * @param jobId
   */
  case class RestRequestJobInfo(jobId: String) extends RestMessage

  case class RestResponseJobInfo(
                                  jobId: String,
                                  jobState: JobState //TODO populate response later
                                  ) extends RestMessage

  /**
   * POST /netflow/v1/jobs
   *
   */
  case class RestRequestSubmitJob(jobDesc: JobDescription) extends RestMessage

  case class RestResponseSubmitJobSuccess(
                                           jobId: String,
                                           message: String) extends RestMessage

  /**
   * DELETE /netflow/v1/jobs/<jobId>
   *
   * @param jobId
   */
  case class RestRequestKillJob(jobId: String) extends RestMessage

  case class RestResponseKillJobSuccess(
                                         jobId: String,
                                         message: String) extends RestMessage

  // message of failure whatever the request was
  case class RestRequestFailed(message: String) extends RestMessage
}