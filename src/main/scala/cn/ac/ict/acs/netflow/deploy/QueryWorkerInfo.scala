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

import scala.collection.mutable

import akka.actor.ActorRef

import cn.ac.ict.acs.netflow.util.Utils

class QueryWorkerInfo(
    val id: String,
    val host: String,
    val port: Int,
    val cores: Int,
    val memory: Int,
    val actor: ActorRef,
    val webUiPort: Int)
  extends Serializable {

  Utils.checkHost(host, "Expected hostname")
  assert (port > 0)

  @transient var queries: mutable.HashMap[String, QueryInfo] = _
  @transient var state: QueryWorkerState.Value = _
  @transient var coresUsed: Int = _
  @transient var memoryUsed: Int = _

  @transient var lastHeartbeat: Long = _

  init()

  def coresFree: Int = cores - coresUsed
  def memoryFree: Int = memory - memoryUsed

  private def init(): Unit = {
    queries = new mutable.HashMap
    state = QueryWorkerState.ALIVE
    coresUsed = 0
    memoryUsed = 0
    lastHeartbeat = System.currentTimeMillis()
  }

  def hostPort: String = {
    assert (port > 0)
    host + ":" + port
  }

  def setState(state: QueryWorkerState.Value) = {
    this.state = state
  }

  def addQuery(query: QueryInfo) {
    queries(query.id) = query
    memoryUsed += query.desc.mem
    coresUsed += query.desc.cores
  }

  def removeQuery(query: QueryInfo) {
    queries -= query.id
    memoryUsed -= query.desc.mem
    coresUsed -= query.desc.cores
  }

}
