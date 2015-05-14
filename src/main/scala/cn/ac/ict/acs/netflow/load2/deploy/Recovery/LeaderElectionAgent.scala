/*
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

package cn.ac.ict.acs.netflow.load2.deploy.Recovery

import cn.ac.ict.acs.netflow.NetFlowConf
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.leader.{LeaderLatch, LeaderLatchListener}
import org.apache.spark.Logging

/** A LeaderElectionAgent tracks current master and is a common interface for all election Agents. */
trait LeaderElectionAgent {
  val masterActor: LeaderElectable
  def stop() {} // to avoid noops in implementations.
}

trait LeaderElectable {
  def appointLeader()
  def revokeLeadership()
}

/**
 * Single-node implementation of LeaderElectionAgent
  * we're initially and always the leader.
  * */
private[Recovery] class MonarchyLeaderAgent(val masterActor: LeaderElectable)
  extends LeaderElectionAgent {
  masterActor.appointLeader()
}

/**
 * Zookeeper leader Agent
 * @param masterActor
 * @param conf
 */
private[Recovery] class ZooKeeperLeaderElectionAgent(
                               val masterActor: LeaderElectable,
                               conf: NetFlowConf)
  extends LeaderLatchListener with LeaderElectionAgent with Logging  {

  val WORKING_DIR = conf.get("netflow.deploy.zookeeper.dir", "/netflow") + "/leader_election"

  private var zk: CuratorFramework = _
  private var leaderLatch: LeaderLatch = _
  private var status = LeadershipStatus.NOT_LEADER

  start()

  private def start() {
    logInfo("Starting ZooKeeper LeaderElection agent")
    val ZK_URL = conf.get("netflow.deploy.zookeeper.url")
    zk = ZookeeperCuratorUtil.newClient(conf)
    leaderLatch = new LeaderLatch(zk, WORKING_DIR)
    leaderLatch.addListener(this)
    leaderLatch.start()
  }

  override def stop() {
    leaderLatch.close()
    zk.close()
  }

  override def isLeader() {
    synchronized {
      // could have lost leadership by now.
      if (!leaderLatch.hasLeadership) {
        return
      }

      logInfo("We have gained leadership")
      updateLeadershipStatus(true)
    }
  }

  override def notLeader() {
    synchronized {
      // could have gained leadership by now.
      if (leaderLatch.hasLeadership) {
        return
      }

      logInfo("We have lost leadership")
      updateLeadershipStatus(false)
    }
  }

  private def updateLeadershipStatus(isLeader: Boolean) {
    if (isLeader && status == LeadershipStatus.NOT_LEADER) {
      status = LeadershipStatus.LEADER
      masterActor.appointLeader()
    } else if (!isLeader && status == LeadershipStatus.LEADER) {
      status = LeadershipStatus.NOT_LEADER
      masterActor.revokeLeadership()
    }
  }

  private object LeadershipStatus extends Enumeration {
    type LeadershipStatus = Value
    val LEADER, NOT_LEADER = Value
  }
}
