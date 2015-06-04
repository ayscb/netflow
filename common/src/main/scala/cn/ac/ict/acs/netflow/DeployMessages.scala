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

import cn.ac.ict.acs.netflow.util.Utils

sealed trait DeployMessage extends Serializable

/**
 * Contains Messages sent between deploy members
 */
object DeployMessages {

  // Broker to Master

  case class RegisterBroker(
      id: String,
      host: String,
      port: Int,
      restPort: Int)
    extends DeployMessage {
    Utils.checkHost(host, "Required hostname")
    assert (port > 0)
  }

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

  case class BrokerStateResponse(brokerId: String) extends DeployMessage

  // Master to Worker

  case class RegisteredBroker(masterUrl: String, masterWebUiUrl: String) extends DeployMessage

  case class RegisterBrokerFailed(message: String) extends DeployMessage

  case class ReconnectBroker(masterUrl: String) extends DeployMessage

  // Broker Internal
  case object ReregisterWithMaster extends DeployMessage

  // Send during master recovery procedure
  case class MasterChanged(masterUrl: String, masterWebUrl: String)

  case object SendHeartbeat

  // Master to Worker( loadWorker | loadQuery | receiver )
  case class RegisteredWorker(masterUrl: String, masterWebUiUrl: String) extends DeployMessage

  case class RegisterWorkerFailed(message: String) extends DeployMessage

  case class ReconnectWorker(masterUrl: String) extends DeployMessage


}
