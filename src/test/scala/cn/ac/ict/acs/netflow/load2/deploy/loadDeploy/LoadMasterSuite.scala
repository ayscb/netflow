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
package cn.ac.ict.acs.netflow.load2.deploy.loadDeploy

import akka.actor.Address
import cn.ac.ict.acs.netflow.NetFlowConf
import org.scalatest.FunSuite

/**
 * Created by ayscb on 2015/5/21.
 */
class LoadMasterSuite extends FunSuite {
  val conf = new NetFlowConf()

  test("toAkkaUrl") {
    val akkaUrl = LoadMaster.toAkkaUrl("netflow://1.2.3.4:1234", "akka.tcp")
    print(akkaUrl)
    assert("akka.tcp://netflowLoadMaster@1.2.3.4:1234/user/LoadMaster" === akkaUrl)
  }

  test("toAkkaAddress") {
    val address = LoadMaster.toAkkaAddress("netflow://1.2.3.4:1234","akka.tcp")
    assert(Address("akka.tcp", "netflowLoadMaster", "1.2.3.4", 1234) === address)
  }

}
