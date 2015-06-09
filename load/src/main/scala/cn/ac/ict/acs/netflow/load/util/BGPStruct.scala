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
package cn.ac.ict.acs.netflow.load.util

/**
 * two level index struct, x,y,a,b for example
 *
 * first index store the x.y full offset , so there should be 65536 value in sequence array
 * second index store the a full offset, so there should be 256 index in sequence arrays
 *
 */
class BGPStruct {
  class Data {}
  case class firstInfo(dataInfo: Data, morePrefix: Boolean, secondIndex: Option[secondInfo])
  case class secondInfo(dataInfo: Data, morePrefix: Boolean, trieIndex: Option[trieTree])
  case class trieTree()

  // store x.y
  val firstIndex = new Array[firstInfo](65536)

  // store a
  val secondIndex = new Array[secondInfo](256)

}
