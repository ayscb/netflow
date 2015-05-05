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

case class Command(
    queryString: Option[String],
    queryPath: Option[String],
    sparkMasterUrl: String,
    mainClass: String,
    jar: String, //path to jar which contains main class
    deployMode: String = "cluster",
    appName: String,
    driverMemory: Option[String],
    driverJavaOpts: Option[String],
    driverLibPath: Option[String],
    driverClassPath: Option[String],
    driverCores: Option[Int],
    executorMemory: Option[String],
    supervise: Boolean = true,
    totalExecutorCores: Option[Int]) {

  require((queryString.isDefined && queryPath.isEmpty) ||
    (queryString.isEmpty && queryPath.isDefined))

  override def toString(): String = {
    val sb = new StringBuilder(200)
    sb ++= s"--master $sparkMasterUrl --class $mainClass "
    sb ++= s"--deploy-mode $deployMode --name $appName "
    driverMemory.foreach(dm => sb ++= s"--driver-memory $dm ")
    driverJavaOpts.foreach(djo => sb ++= s"--driver-java-options $djo ")
    driverLibPath.foreach(dlp => sb ++= s"--driver-library-path $dlp ")
    driverClassPath.foreach(dcp => sb ++= s"--driver-class-path $dcp ")
    driverCores.foreach(dc => sb ++= s"--driver-cores $dc ")
    executorMemory.foreach(em => sb ++= s"--executor-memory $em ")
    if (supervise) {
      sb ++= "--supervise "
    }
    totalExecutorCores.foreach(tec => sb ++= s"--total-executor-cores $tec ")
    sb ++= s"$jar "
    queryString.foreach(qs => sb ++= s"$qs ")
    queryPath.foreach(qp => sb ++= s"$qp ")

    sb.toString()
  }
}
