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
package cn.ac.ict.acs.netflow.util

import java.util.concurrent.TimeUnit

import cn.ac.ict.acs.netflow.NetFlowConf
import org.joda.time.DateTime
import org.scalatest.{Matchers, FunSuite}

import cn.ac.ict.acs.netflow.util.TimeUtils._

class TimeUtilsSuite extends FunSuite with Matchers {

  test("invalid suffix") {
    intercept[NumberFormatException] {
      parseTimeString("10fake", TimeUnit.SECONDS)
    }
  }

  test("default suffix") {
    assert(parseTimeString("10", TimeUnit.SECONDS) === 10)
  }

  test("convert to upper unit") {
    assert(parseTimeString("10ms", TimeUnit.SECONDS) === 0)
  }

  test("convert to lower unit") {
    assert(parseTimeString("10s", TimeUnit.MILLISECONDS) === 10000)
  }

  test("generate path") {
    val t1 = DateTime.parse("2015-10-11,12:32:11", showFormat)
    assertResult("/2015/10/11/12/32") {
      getTimeBasePathBySeconds(new NetFlowConf, t1.getMillis / 1000)
    }
  }

}
