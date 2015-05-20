package cn.ac.ict.acs.netflow.master

import akka.serialization.Serialization
import cn.ac.ict.acs.netflow._

class ZKRecoveryModeFactory(conf: NetFlowConf, serializer: Serialization)
  extends RecoveryModeFactory(conf, serializer) {
  def createPersistenceEngine() = new QueryMasterZKPersistenceEngine(conf, serializer)

  def createLeaderElectionAgent(master: LeaderElectable) =
    new ZooKeeperLeaderElectionAgent(master, conf)
}


class QueryMasterZKPersistenceEngine(conf: NetFlowConf, serialization: Serialization)
  extends ZooKeeperPersistenceEngine(conf, serialization) with MasterPersistenceEngine

class QueryMasterBHPersistenceEngine extends BlackHolePersistenceEngine with MasterPersistenceEngine
