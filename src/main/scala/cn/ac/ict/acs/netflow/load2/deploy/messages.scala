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
package cn.ac.ict.acs.netflow.load2.deploy

import cn.ac.ict.acs.netflow.QueryDescription
import cn.ac.ict.acs.netflow.util.Utils
import org.apache.spark.deploy.master.{WorkerInfo, ApplicationInfo}

//******************** Deploy Message *********************//
sealed trait DeployMessage extends Serializable

/** Contains Messages sent between deploy members */
object DeployMessages {

  case class RegisterWorker (
                             id: String,
                             host: String,
                             port: Int,
                             cores: Int,
                             memory: Int,
                             webUiPort: Int,
                             udpPort:Int)
    extends DeployMessage {
    Utils.checkHost(host, "Required hostname")
    assert (port > 0 && udpPort > 0)
  }

  case class Heartbeat(workerId: String) extends DeployMessage

  // Master to Worker( loadWorker | loadQuery | receiver )
  case class RegisteredWorker(masterUrl: String, masterWebUiUrl: String) extends DeployMessage
  case class RegisterWorkerFailed(message: String) extends DeployMessage
  case class ReconnectWorker(masterUrl: String) extends DeployMessage

  // Worker internal
  case object ReregisterWithMaster // used when a worker attempts to reconnect to a master
  case object WorkDirCleanup // Sent to Worker actor periodically for cleaning up app folders


  // Send during master recovery procedure
  case class MasterChanged(masterUrl: String, masterWebUrl: String)

}


//********************* Master Message ********************//
sealed trait MasterMessage extends Serializable

private[deploy] object MasterMessage {

  // LeaderElectionAgent to Master
  case object AppointedAsLeader
  case object RevokedLeadership

  // Actor System to Master
  case object CheckForWorkerTimeOut
  case object CompleteRecovery
  case object BoundPortsRequest
  case class BoundPortsResponse(actorPort: Int, webUIPort: Int)
}

/** Contains messages seen only by the QueryMaster and its associated entities. */
object QueryMasterMessages extends MasterMessage {

}

/** Contains messages sent only by the LoadMaster and its associated entities**/
object LoadMasterMessage extends MasterMessage {

  // Actor System to Master
  case object getBGP  // fet the BGPs from BGP drivers( or Internet etc.)

  // Master to Worker
  case class updateBGP( bgpIds : Array[Int], bgpDatas: Array[Array[Byte]])

  // Master to worker to update the
  //case class updateDestWorker( host : String, port : Int)

  //Master to worker to tell worker to combine the parquets
  case object CombineParquet

  //Master to Master ( schedule )
  case object NeedToCombineParquet

  // master to worker ( only when the collector's number is 1 who is connected with the worker we want to adjust )
  case object AdjustThread
}


//********************* worker Message ********************//
sealed trait WorkerMessage extends Serializable

private[deploy] object WorkerMessage{
  case object SendHeartbeat
}

object QueryWorkerMessage extends WorkerMessage {

  case class RegisterQuery(queryId: String)
  case object RegisterQueryFailed
  case class LaunchQuery(
                          masterUrl: String,
                          queryId: String,
                          queryDesc: QueryDescription)
  case class KillQuery(queryId: String)
}

object LoadWorkerMessage extends WorkerMessage {
  case class CacheInfo(workId: String, used: Int, remain: Int)

  // worker to ResolvingActor
  case class LaunchResolvingThread ( threadNum : Int )
  case class AdjustResolvingThread ( threadNum : Int )
  case object CloseAllResolvingThread

  // worker to UDPActor
  case object UDPReceiveStart
  case object UDPReceiveStop

  // worker(LoadBalanceStrategy) to master
  case class BuffersWarn ( workerHost: String )
  case class BufferOverFlow ( workerHost:String )

  //worker to master
  case object CombineFinished
}


