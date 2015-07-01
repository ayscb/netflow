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
package cn.as.ict.acs.netflow.load.master

import cn.ac.ict.acs.netflow.load.Rule

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Created by ayscb on 6/24/15.
 */
object Test_loadmaster {

  private val destAddr = mutable.LinkedHashSet.empty[String]
  private val destAddr2idx = mutable.HashMap.empty[String, Int]
  private val srcMapping = mutable.HashMap.empty[String, Int]

  private def notifyRulesToAllWorkers(rules: ArrayBuffer[Rule]): Unit = {
    rules.foreach(f = rule => {
      if (destAddr.contains(rule.dest)) {
        val idx: Int = destAddr2idx(rule.dest)
        srcMapping(rule.src) = idx
      } else {
        val idx = destAddr.size
        destAddr2idx(rule.dest) = idx
        srcMapping(rule.src) = idx
        destAddr += rule.dest
      }
    })

    val key = srcMapping.iterator.map(x => x._1 + ":" + x._2).mkString(";")
    val value = destAddr.iterator.mkString(";")
    println(key)
    println(value)
  }

  private def pushRuleToWorker(): Unit = {
    val key = srcMapping.iterator.map(x => x._1 + ":" + x._2).mkString(";")
    val value = destAddr.iterator.mkString(";")
    println(key)
    println(value)
  }

  def main(args: Array[String]) {
    val rules = new ArrayBuffer[Rule]()
    rules += Rule("asd","val1")
    rules += Rule("asdf","val1")
    rules += Rule("asdg","val2")
    rules += Rule("asdh","val1")
    rules += Rule("asdj","val3")
    rules += Rule("asdk","val2")
    rules += Rule("asdl","val4")
    notifyRulesToAllWorkers(rules)
    pushRuleToWorker()
  }
}
