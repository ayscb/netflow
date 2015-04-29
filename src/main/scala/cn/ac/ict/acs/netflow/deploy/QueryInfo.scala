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

import cn.ac.ict.acs.netflow.deploy.QueryState.QueryState
import org.joda.time.DateTime

case class QueryInfo(
    jobId: String,
    id: String,
    startTime: Long,
    desc: QueryDescription,
    submitDate: DateTime) {

  @transient var state: QueryState = QueryState.SUBMITTED
  /* If we fail when launching the driver, the exception is stored here. */
  @transient var exception: Option[Exception] = None
  /* Most recent worker assigned to this driver */
  @transient var worker: Option[QueryWorkerInfo] = None

}
