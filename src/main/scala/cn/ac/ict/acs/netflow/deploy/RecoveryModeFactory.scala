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

import akka.serialization.Serialization
import cn.ac.ict.acs.netflow.NetFlowConf

/**
 * Implementation of this class can be plugged in as recovery mode alternative for NetFlow
 */
abstract class RecoveryModeFactory(conf: NetFlowConf, serializer: Serialization) {

  /**
   * PersistenceEngine defines how the persistent data(Information about worker, query etc..)
   * is handled for recovery.
   *
   */
  def createPersistenceEngine(): PersistenceEngine

  /**
   * Create an instance of LeaderAgent that decides who gets elected as master.
   */
  def createLeaderElectionAgent(master: LeaderElectable): LeaderElectionAgent

}

private[netflow] class ZooKeeperRecoveryModeFactory(conf: NetFlowConf, serializer: Serialization)
  extends RecoveryModeFactory(conf, serializer) {
  def createPersistenceEngine() = new ZooKeeperPersistenceEngine(conf, serializer)

  def createLeaderElectionAgent(master: LeaderElectable) =
    new ZooKeeperLeaderElectionAgent(master, conf)
}
