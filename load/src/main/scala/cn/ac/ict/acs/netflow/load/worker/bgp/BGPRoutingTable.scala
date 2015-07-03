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
package cn.ac.ict.acs.netflow.load.worker.bgp

object BGPRoutingTable {

  type DST_ADDR = (Array[Byte], Array[Byte])

  def search(dst_addr: DST_ADDR): BGPTuple = {
    new BGPTuple(Array(
      "192.168.1.1".getBytes,
      "192.168.1.1".getBytes, null,
      "192.168.2.1".getBytes, null,
      "as_path", "community", "adjacent_as", "self_as"))
  }

  def update(tuple: BGPTuple): Unit = ???

}

case class BGPTuple(fields: Array[Any])
