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
package cn.ac.ict.acs.netflow.ha

import scala.collection.JavaConversions._

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.KeeperException

import cn.ac.ict.acs.netflow.{Logging, NetFlowConf}

object NetFlowCuratorUtil extends Logging {

  val ZK_CONNECTION_TIMEOUT_MILLIS = 15000
  val ZK_SESSION_TIMEOUT_MILLIS = 60000
  val RETRY_WAIT_MILLIS = 5000
  val MAX_RECONNECT_ATTEMPTS = 3

  def newClient(conf: NetFlowConf): CuratorFramework = {
    val ZK_URL = conf.get("netflow.deploy.zookeeper.url")
    val zk = CuratorFrameworkFactory.newClient(ZK_URL,
      ZK_SESSION_TIMEOUT_MILLIS, ZK_CONNECTION_TIMEOUT_MILLIS,
      new ExponentialBackoffRetry(RETRY_WAIT_MILLIS, MAX_RECONNECT_ATTEMPTS))
    zk.start()
    zk
  }

  def mkdir(zk: CuratorFramework, path: String) {
    if (zk.checkExists().forPath(path) == null) {
      try {
        zk.create().creatingParentsIfNeeded().forPath(path)
      } catch {
        case nodeExist: KeeperException.NodeExistsException =>
        // do nothing, ignore node existing exception.
        case e: Exception => throw e
      }
    }
  }

  def deleteRecursive(zk: CuratorFramework, path: String) {
    if (zk.checkExists().forPath(path) != null) {
      for (child <- zk.getChildren.forPath(path)) {
        zk.delete().forPath(path + "/" + child)
      }
      zk.delete().forPath(path)
    }
  }

}
