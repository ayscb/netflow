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

import cn.ac.ict.acs.netflow.deploy.JobType.JobType
import cn.ac.ict.acs.netflow.deploy.QueryState.QueryState
import cn.ac.ict.acs.netflow.util.Utils

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

/**
 * Contains Messages sent between deploy members
 */
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

  case class QueryStateChange(
      queryId: String,
      state: QueryState,
      exception: Option[Exception])

  case class WorkerSchedulerStateResponse(id: String, queryIds: Seq[String])

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

  // Master internal & worker to Master
  case class KillQueryResponse(queryId: String, success: Boolean, message: String)

}

object Messages {

  case class SubmitQuery(jobId: String, tpe: JobType, queryDesc: QueryDescription)

  case object SendHeartbeat

  // firstShot is millis since epoch
  case class RegisterJob(
      tpe: JobType,
      firstShot: Long,
      interval: Option[Long],
      desc: QueryDescription) {

    require({
      if (tpe == JobType.REPORT) {
        interval.isDefined
      } else {
        !interval.isDefined && firstShot == 0
      }
    }, "ReportJob should define interval as execution cycle" +
      " Meanwhile, online or adhoc job should not utilize it")
  }

  case class CancelJob(jobId: String)

  case class RegisterJobResponse(success: Boolean, jobId: Option[String], message: String)

  case class CancelJobResponse(jobId: String, success: Boolean, message: String)

  case class LaunchQuery(
    queryId: String,
    queryDesc: QueryDescription)

  case class KillQuery(queryId: String)
}
