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
package cn.ac.ict.acs.netflow.load2.DataReceive

import java.nio.ByteBuffer
import java.util
import java.util.concurrent.Exchanger

/**
 * Created by ayscb on 2015/4/23.
 */

abstract class DoubleCache[ T ]( ) {
  protected val exchanger = new Exchanger[T]()
  protected var buffer1 : T = _
  protected var buffer2 : T = _

  protected var runnable1 : Runnable = _
  protected var runnable2 : Runnable = _

  def run() :Unit ={
    if( runnable1 != null){
      new Thread(runnable1).start()
      new Thread(runnable2).start()
    }
  }
}


class DobuleBufferCache (val byteSize : Int,
                         product :( ByteBuffer ) => Int,
                         consume: ( ByteBuffer) => Unit)
  extends DoubleCache[ByteBuffer]{

  buffer1 = ByteBuffer.allocate(byteSize)
  buffer2 = ByteBuffer.allocate(byteSize)

  runnable1 = new Runnable {
    override def run(): Unit = {
      var currentBuffer = buffer1
      currentBuffer.clear()
      try {
        while ( currentBuffer != null ) {
          product(currentBuffer) // put the data into buffer
          if ( !currentBuffer.hasRemaining ){
            currentBuffer = exchanger.exchange(currentBuffer)
            currentBuffer.clear()
          }
        }
      }
      catch {
        case e: InterruptedException => print(e.getMessage)
      }
    }
  }

  runnable2 = new Runnable {
    override def run(): Unit = {
      var currentBuffer = buffer2
      currentBuffer.flip()
      try {
        while ( currentBuffer != null ) {
          if (!currentBuffer.hasRemaining){
            currentBuffer = exchanger.exchange(currentBuffer)
            currentBuffer.flip()
          }
          consume(currentBuffer)      // put the data into buffer
        }
      }catch {
        case e: InterruptedException => print(e.getMessage)
      }
    }
  }
}