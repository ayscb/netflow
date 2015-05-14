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
package cn.ac.ict.acs.netflow.deploy.qmaster

import akka.actor.ActorRef

import cn.ac.ict.acs.netflow.util.Utils

class BrokerInfo(
  val id: String,
  val host: String,
  val port: Int,
  val restPort: Int,
  val actor: ActorRef)
  extends Serializable {

  Utils.checkHost(host, "Expected hostname")
  assert (port > 0)

  init()

  @transient var state: BrokerState.Value = _
  @transient var lastHeartbeat: Long = _

  private def init(): Unit = {
    state = BrokerState.ALIVE
    lastHeartbeat = System.currentTimeMillis()
  }

  def hostPort(): String = {
    host + ":" + port + ":" + restPort
  }

  def setState(state: BrokerState.Value) = {
    this.state = state
  }
}
