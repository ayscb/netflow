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
package cn.ac.ict.acs.netflow.load2.deploy.LoadDeploy

import java.nio.ByteBuffer

/**
 * Created by ayscb on 2015/5/4.
 */
class netflowBuffers( bufferSize : Int , allocSize : Int = 1500 ) {

  var productRate : Int = 0
  var consumeRate : Int = 0

  val udpPackageNum = Math.ceil( bufferSize / allocSize ).asInstanceOf[Int]

  val buffers = new Array[ByteBuffer](udpPackageNum)
  val bufferConsumed = new Array[Boolean](udpPackageNum)
  for( i <- 0 until bufferConsumed.size)
    bufferConsumed(i) = true

  var productIdx = 0
  var consumeIdx = 0

  var startWTime : Long = 0
  var totalProNum = 0
  var startRTime : Long = 0
  var totalComNum = 0

  private def checkEmpty : Boolean = productIdx == consumeIdx
  private def checkFull : Boolean = ( productIdx + 1 ) % udpPackageNum == consumeIdx

  def getWriteBufer : (Int, ByteBuffer) = synchronized {
    if( checkFull ) return null

    if( buffers(productIdx) == null ){
      buffers(productIdx) = ByteBuffer.allocate(1500)
    }else {
      if (!bufferConsumed(productIdx)) return null
    }

    val buffTuple = ( productIdx,buffers(productIdx) )
    buffTuple._2.clear()
    productIdx = ( productIdx + 1 )% udpPackageNum
    buffTuple
  }

  def writeBufferOver( bufferIdx : Int) = synchronized{
    if( startWTime == 0)
      startWTime = System.nanoTime()
    totalProNum += 1

    bufferConsumed(bufferIdx) = false
  }

  def getWriteRate :Long =
    if( totalProNum != 0 )
      ( System.nanoTime() - startWTime ) / totalProNum / 1000
  else
      0

  def getReadBuffer : (Int, ByteBuffer) = synchronized {
    if( checkEmpty ) return null
    if( !bufferConsumed(consumeIdx) ){
      val buffTupe =( consumeIdx, buffers( consumeIdx ))
      buffTupe._2.flip()
      consumeIdx  = ( consumeIdx + 1 ) % udpPackageNum
      return buffTupe
    }else
      return null
  }

  def ReadBufferOver( bufferIdx : Int) = synchronized{
    if( startRTime == 0)
      startRTime = System.nanoTime()
    totalComNum += 1

    bufferConsumed(bufferIdx) = true
  }

  def getReadRate : Long =
    if(totalComNum != 0)
      (System.nanoTime() - startRTime) / totalComNum / 1000
  else
      0

  def getCurrentRelativeRate = productIdx - consumeIdx
}

object netflowBuffers {
  val buffers = new netflowBuffers( 150,10 )

  def producer = new Thread("producer " + idx ){
    override def run(): Unit = {
        while( true ){
          val buffTupe = buffers.getWriteBufer
          if( buffTupe == null ){
            println("  " + Thread.currentThread().getName + " is running, but bufferList is full !!")
            Thread.sleep(100)
          }
          else
          {
            println( "  " +  Thread.currentThread().getName + " is running, and will write to list idx = " + buffTupe._1)

            println( "  " +  Thread.currentThread().getName + " write data: " + Thread.currentThread().getId)
            buffTupe._2.putLong(Thread.currentThread().getId)
            Thread.sleep(100)

            println( "  " +  Thread.currentThread().getName + " write over ")
            buffers.writeBufferOver(buffTupe._1)

          }
          println()
        }
    }
  }

  var idx = 0
  def consumer = new Thread("consumer " + idx ){
    override def run(): Unit = {
      while( true ){
        val buffTupe = buffers.getReadBuffer
        if( buffTupe == null )
          println(Thread.currentThread().getName + " is running, but bufferList is empty !!")
        else{
          println( Thread.currentThread().getName + " running , read list idx = " + buffTupe._1)
          println( Thread.currentThread().getName + " read data: " + buffTupe._2.getLong)
          buffers.ReadBufferOver(buffTupe._1)
          Thread.sleep(50)
          println( Thread.currentThread().getName + " read data over ")
        }

        println()
      }
    }
  }

  // statistics

  def main(args: Array[String]) {
    println("total package num " + buffers.udpPackageNum)
    println()
    idx = 0
    for( i <- 0 until 7){
      producer.start()
      idx += 1
    }

    idx = 10
    for( i <- 0 until 3){
      consumer.start()
      idx += 1
    }

    while (true){
      println("&&&& main thread &&&& current relative rate " + buffers.getCurrentRelativeRate)
      println("&&&& main thread &&&& read rate - write rate :  " + ( buffers.getReadRate - buffers.getWriteRate))
      Thread.sleep(100)
    }
  }
}
