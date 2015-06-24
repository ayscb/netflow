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

  /**
   * load balance message
   */
  // worker(LoadBalanceStrategy) to master
  case class BuffersWarn(workerIP: String)
  case class BufferOverFlow(workerIP: String)
  case class BufferSimpleReport(workerIP: String, usageRate: Double)
  case class BufferWholeReport(workerIP: String, usageRate: Double, maxSize: Int, curSize: Int)

  // Master to worker [ to get buffer simple info ]
  case object BufferInfo

  // worker to worker [ to regular whole report ]
  case object BuffHeatBeat

  // master to worker to adjust current worker thread
  case object AdjustThread

  /**
   * combine message
   */
  // worker to master
  case class CombineFinished(status: CombineStatus.Value)

  // Master to worker [to tell worker to combine the parquets]
  case class CombineParquet(fileStamp: Long)

  // worker's loadPool to worker
  // [to tell worker current thread has been written, and should be combine]

  // worker to master
  // [to tell master combine this directory]
  // TODO: Rename this to `ParquetWriterClosed`
  case class CloseParquet(fileStamp: Long)

  /**
   * bgp message
   */
  // fet the BGPs from BGP drivers( or Internet etc.)
  case object getBGP

  // Master to Worker
  case class updateBGP(bgpIds: Array[Int], bgpDatas: Array[Array[Byte]])

  /**
   * receiver message
   */
  case class DeleReceiver(receiverIP: String)
  case class DeleWorker(workerIP: String, port: Int)
  case class RequestWorker(receiverIP: String)

}

object CombineStatus extends Enumeration {
  type CombineStatus = Value
  val FINISH, DIRECTORY_NOT_EXIST, UNKNOWN_DIRECTORY, IO_EXCEPTION, PARTIAL_FINISH = Value
}
