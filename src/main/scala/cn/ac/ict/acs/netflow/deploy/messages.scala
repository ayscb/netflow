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

<<<<<<< HEAD
import cn.ac.ict.acs.netflow.QueryDescription
import cn.ac.ict.acs.netflow.util.Utils
import org.apache.spark.deploy.master.{WorkerInfo, ApplicationInfo}

sealed trait QueryMasterMessages extends Serializable

/** Contains messages seen only by the Master and its associated entities. */
object QueryMasterMessages {

  // LeaderElectionAgent to Master

  case object AppointedAsLeader

  case object RevokedLeadership

  // Actor System to Master

  case object CheckForWorkerTimeOut

//  case class BeginRecovery(
//    storedApps: Seq[ApplicationInfo], storedWorkers: Seq[WorkerInfo])

  case object CompleteRecovery

  case object BoundPortsRequest extends QueryMasterMessages

  case class BoundPortsResponse(actorPort: Int, webUIPort: Int)
    extends QueryMasterMessages
}

sealed trait DeployMessage extends Serializable

/** Contains Messages sent between deploy members */
object DeployMessages {

  // Worker to Master

  case class RegisterWorker(
      id: String,
      host: String,
      port: Int,
      cores: Int,
      memory: Int,
      webUiPort: Int)
    extends DeployMessage {
    Utils.checkHost(host, "Required hostname")
    assert (port > 0)
  }

  case class Heartbeat(workerId: String) extends DeployMessage

  // Master to Worker

  case class RegisteredWorker(masterUrl: String, masterWebUiUrl: String) extends DeployMessage

  case class RegisterWorkerFailed(message: String) extends DeployMessage

  case class ReconnectWorker(masterUrl: String) extends DeployMessage

  // Worker internal

  case object ReregisterWithMaster // used when a worker attempts to reconnect to a master

  case object WorkDirCleanup // Sent to Worker actor periodically for cleaning up app folders

  // Master to Worker & QueryDriver?

  // Send during master recovery procedure
  case class MasterChanged(masterUrl: String, masterWebUrl: String)

}

sealed trait LoadMasterMessage extends Serializable

/** Contains messages sent only by the LoadMaster and its associated entities**/
object LoadMasterMessage{
  // LeaderElectionAgent to Master

  case object ElectedLeader

  case object RevokedLeadership

  // Actor System to Master

  case object CheckForWorkerTimeOut

  case object CompleteRecovery

  case object BoundPortsRequest

  case class BoundPortsResponse(actorPort: Int, webUIPort: Int, restPort: Option[Int])
}

object Messages {

  case object SendHeartbeat

  case class RegisterQuery(queryId: String)
=======
import cn.ac.ict.acs.netflow.deploy.qmaster.JobState._
import cn.ac.ict.acs.netflow.deploy.qmaster.JobType
import cn.ac.ict.acs.netflow.deploy.qmaster.JobType._

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
   * @param tpe
   * @param firstShot millis since epoch
   * @param interval period between two scheduling if it is a report job
   * @param cmd
   */
  case class RestRequestSubmitJob(
      tpe: JobType,
      firstShot: Long,
      interval: Option[Long],
      cmd: Command) extends RestMessage {

    require({
      if (tpe == JobType.REPORT) {
        interval.isDefined
      } else {
        !interval.isDefined && firstShot == 0
      }
    }, "ReportJob should define interval as execution cycle" +
      " Meanwhile, online or adhoc job should not utilize it")
  }

  case class RestResponseSubmitJobSuccess(
      jobId: String,
      message: String) extends RestMessage
>>>>>>> sql-1/master

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
