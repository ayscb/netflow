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
package cn.ac.ict.acs.netflow.deploy.qmaster

import cn.ac.ict.acs.netflow.deploy.Command
import cn.ac.ict.acs.netflow.deploy.qmaster.JobType.JobType

object JobType extends Enumeration {
  type JobType = Value
  
  val ADHOC, REPORT, ONLINE = Value
}

/**
 *
 * @param id System wide unique id
 * @param tpe
 * @param first first launch time since epoch
 * @param interval
 * @param cmd
 */
case class Job(
    id: String,
    tpe: JobType,
    first: Long,
    interval: Option[Long],
    cmd: Command) { //TODO: should ONLINE query require much more resource?

}
