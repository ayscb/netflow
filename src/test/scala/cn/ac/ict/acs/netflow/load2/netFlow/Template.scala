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
package cn.ac.ict.acs.netflow.load2.netFlow

import java.nio.ByteBuffer

/**
 * Created by ayscb on 2015/5/12.
 */
class Template extends org.scalatest.FunSuite {

  val keyList = Array(1,2,3,4,5,6,7,8,9,10,11,12,13)

  val valueBuff = {
    val buffer = ByteBuffer.allocate(keyList.length * 2)
    for( i <- 0 until keyList.length){
      buffer.putShort( ( i + 21 ).asInstanceOf[Short] )
    }
    buffer
  }

  test(" check the UnSafeTemplate "){

    val unsafeTmp = new UnSafeTemplate(0,13)
    unsafeTmp.updateTemplate(0,13,valueBuff)

//    val thread = new Thread(){
//      var idx = 0
//      val iter = unsafeTmp.iterator
//      while( iter.hasNext){
//        val data = iter.next()
//        assert(data._1 == keyList(idx))
//        assert(data._2 == keyList(idx) + 21)
//        idx += 1
//      }
//    }
  }
}
