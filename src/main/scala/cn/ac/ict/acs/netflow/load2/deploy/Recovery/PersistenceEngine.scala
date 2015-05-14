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
package cn.ac.ict.acs.netflow.load2.deploy.Recovery

import java.io.{DataInputStream, File, FileInputStream, FileOutputStream}

import akka.serialization.Serialization
import cn.ac.ict.acs.netflow.NetFlowConf
import cn.ac.ict.acs.netflow.load2.NetFlowArgument
import cn.ac.ict.acs.netflow.load2.deploy.LoadDeploy.LoadWorkerInfo
import org.apache.curator.framework.CuratorFramework
import org.apache.spark.Logging
import org.apache.zookeeper.CreateMode

import scala.reflect.ClassTag

/**
 * Allows Master to persist any state that is necessary in order to recover from a failure.
 * The following semantics are required:
 *   - addApplication and addWorker are called before completing registration of a new app/worker.
 *   - removeApplication and removeWorker are called at any time.
 * Given these two requirements, we will have all apps and workers persisted, but
 * we might not have yet deleted apps or workers that finished (so their liveness must be verified
 * during recovery).
 *
 * The implementation of this trait defines how name-object pairs are stored or retrieved.
 */
trait PersistenceEngine {

  /**
   * Defines how the object is serialized and persisted. Implementation will
   * depend on the store used.
   */
  def persist(name: String, obj: Object)

  /**
   * Defines how the object referred by its name is removed from the store.
   */
  def unpersist(name: String)

  /**
   * Gives all objects, matching a prefix. This defines how objects are
   * read/deserialized back.
   */
  def read[T: ClassTag](prefix: String): Seq[T]

  final def addWorker(worker: LoadWorkerInfo): Unit = {
    persist("worker_" + worker.id, worker)
  }

  final def removeWorker(worker: LoadWorkerInfo): Unit = {
    unpersist("worker_" + worker.id)
  }

  def close() {}
}

private[Recovery] class BlackHolePersistenceEngine extends PersistenceEngine {

  override def persist(name: String, obj: Object): Unit = {}
  override def unpersist(name: String): Unit = {}
  override def read[T: ClassTag](name: String): Seq[T] = Nil
}

private[Recovery] class FileSystemPersistenceEngine(
            val dir: String,
            val serialization: Serialization)
    extends PersistenceEngine with Logging {
  /**
   * Defines how the object is serialized and persisted. Implementation will
   * depend on the store used.
   */
  override def persist(name: String, obj: Object): Unit = {
    serializeIntoFile(new File(dir + File.separator + name), obj)
  }

  /**
   * Defines how the object referred by its name is removed from the store.
   */
  override def unpersist(name: String): Unit = {
    new File(dir + File.separator + name).delete()
  }

  /**
   * Gives all objects, matching a prefix. This defines how objects are
   * read/deserialized back.
   */
  override def read[T: scala.reflect.ClassTag](prefix: String): Seq[T] = {
    val files = new File(dir).listFiles().filter(_.getName.startsWith(prefix))
    files.map(deserializeFromFile[T])
  }

  private def serializeIntoFile(file: File, value: AnyRef) {
    val created = file.createNewFile()
    if (!created) { throw new IllegalStateException("Could not create file: " + file) }
    val serializer = serialization.findSerializerFor(value)
    val serialized = serializer.toBinary(value)
    val out = new FileOutputStream(file)
    cn.ac.ict.acs.netflow.util.Utils.tryWithSafeFinally {
      out.write(serialized)
    } {
      out.close()
    }
  }

  private def deserializeFromFile[T](file: File)(implicit m: ClassTag[T]): T = {
    val fileData = new Array[Byte](file.length().asInstanceOf[Int])
    val dis = new DataInputStream(new FileInputStream(file))
    try {
      dis.readFully(fileData)
    } finally {
      dis.close()
    }
    val clazz = m.runtimeClass.asInstanceOf[Class[T]]
    val serializer = serialization.serializerFor(clazz)
    serializer.fromBinary(fileData).asInstanceOf[T]
  }

}

private[Recovery] class ZooKeeperPersistenceEngine(
       conf: NetFlowConf,
       val serialization: Serialization)
    extends PersistenceEngine with Logging {

  private val WORKING_DIR = conf.get(NetFlowArgument.ZOOKEEPER_DIR, "/netflow") + "/master_status"
  private val zk: CuratorFramework = ZookeeperCuratorUtil.newClient(conf)

  ZookeeperCuratorUtil.mkdir(zk, WORKING_DIR)

  override def persist(name: String, obj: Object): Unit = {
    serializeIntoFile(WORKING_DIR + "/" + name, obj)
  }

  override def unpersist(name: String): Unit = {
    zk.delete().forPath(WORKING_DIR + "/" + name)
  }

  import scala.collection.JavaConversions._

  override def read[T: ClassTag](prefix: String): Seq[T] = {
    val file = zk.getChildren.forPath(WORKING_DIR).filter(_.startsWith(prefix))
    file.map(deserializeFromFile[T]).flatten
  }

  override def close() = zk.close()

  private def serializeIntoFile(path: String, value: AnyRef) {
    val serializer = serialization.findSerializerFor(value)
    val serialized = serializer.toBinary(value)
    zk.create().withMode(CreateMode.PERSISTENT).forPath(path, serialized)
  }

  private def deserializeFromFile[T](filename: String)(implicit m: ClassTag[T]): Option[T] = {
    val fileData = zk.getData().forPath(WORKING_DIR + "/" + filename)
    val clazz = m.runtimeClass.asInstanceOf[Class[T]]
    val serializer = serialization.serializerFor(clazz)
    try {
      Some(serializer.fromBinary(fileData).asInstanceOf[T])
    } catch {
      case e: Exception => {
        logWarning("Exception while reading persisted file, deleting", e)
        zk.delete().forPath(WORKING_DIR + "/" + filename)
        None
      }
    }
  }
}