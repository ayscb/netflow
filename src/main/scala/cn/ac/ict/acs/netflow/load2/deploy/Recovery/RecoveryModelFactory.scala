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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.ac.ict.acs.netflow.load2.deploy.recovery

import akka.serialization.Serialization
import cn.ac.ict.acs.netflow.{Logging, NetFlowConf}
import cn.ac.ict.acs.netflow.load2.NetFlowArgument

/**
 * Created by ayscb on 2015/4/29.
 */
abstract class StandaloneRecoveryModeFactory(conf: NetFlowConf, serializer: Serialization) {

  /**
   * PersistenceEngine defines how the persistent data(Information about worker, driver etc..)
   * is handled for recovery.
   */
  def createPersistenceEngine(): PersistenceEngine

  /** Create an instance of LeaderAgent that decides who gets elected as master.*/
  def createLeaderElectionAgent(master: LeaderElectable): LeaderElectionAgent
}

/**
 * LeaderAgent in this case is a no-op. Since leader is forever leader as the actual
 * recovery is made by restoring from filesystem.
 */
 class FileSystemRecoveryModeFactory(conf: NetFlowConf, serializer: Serialization)
  extends StandaloneRecoveryModeFactory(conf, serializer) with Logging {

  val RECOVERY_DIR = conf.get(NetFlowArgument.RECOVERY_DIR, "")

  def createPersistenceEngine(): PersistenceEngine = {
    logInfo("Persisting recovery state to directory: " + RECOVERY_DIR)
    new FileSystemPersistenceEngine(RECOVERY_DIR, serializer)
  }

  def createLeaderElectionAgent(master: LeaderElectable): LeaderElectionAgent = {
    new MonarchyLeaderAgent(master)
  }
}

 class ZooKeeperRecoveryModeFactory(conf: NetFlowConf, serializer: Serialization)
  extends StandaloneRecoveryModeFactory(conf, serializer) {

  def createPersistenceEngine(): PersistenceEngine = {
    new ZooKeeperPersistenceEngine(conf, serializer)
  }

  def createLeaderElectionAgent(master: LeaderElectable): LeaderElectionAgent = {
    new ZooKeeperLeaderElectionAgent(master, conf)
  }
}

 class MonarchyRecoveryModeFactory() {
  def createPersistenceEngine(): PersistenceEngine = {
    new BlackHolePersistenceEngine()
  }

  def createLeaderElectionAgent(master: LeaderElectable): LeaderElectionAgent = {
    new MonarchyLeaderAgent(master)
  }

}
