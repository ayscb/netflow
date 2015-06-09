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
package cn.ac.ict.acs.netflow.load

object LoadMessages {

  case class CacheInfo(workId: String, used: Int, remain: Int)

  // worker(LoadBalanceStrategy) to master
  case class BuffersWarn(workerHost: String)
  case class BufferOverFlow(workerHost: String)
  case class BufferReport(workerHost: String, rate: Int)

  // worker to worker
  object BufferBeat

  // worker to master
  case object CombineFinished

  // fet the BGPs from BGP drivers( or Internet etc.)
  case object getBGP

  // Master to Worker
  case class updateBGP(
    bgpIds: Array[Int],
    bgpDatas: Array[Array[Byte]])

  // Master to worker to update the
  // case class updateDestWorker( host : String, port : Int)

  // Master to worker to tell worker to combine the parquets
  case object CombineParquet

  // Master to worker to get buffer info
  case object BufferInfo

  // master to worker

  // only when the collector's number is 1 who is connected with the worker we want to adjust
  case object AdjustThread

}
