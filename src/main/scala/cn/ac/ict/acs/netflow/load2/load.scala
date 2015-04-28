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
package cn.ac.ict.acs.netflow.load2

import java.io._
import java.nio.channels.FileChannel
import java.nio.{ByteOrder, CharBuffer, ByteBuffer}

import cn.ac.ict.acs.netflow.NetFlowConf
import cn.ac.ict.acs.netflow.load2.netFlow.AnalysisFlowData
import cn.ac.ict.acs.netflow.load2.parquetUtil.{NetFlowWriterUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import scala.io.Source


/**
 * Created by ayscb on 2015/4/19.
 */
object load {

  val buffer = ByteBuffer.allocate(1500)

  def main(args: Array[String]) {

//    val path = new Path("/")
//    println(path.toUri)
//    println(path.toUri.getPath)
//    val conf = new Configuration()
//    conf.set("fs.defaultFS","hdfs://192.168.80.110")
//    val fs = path.getFileSystem(conf)
//
//    val status = fs.getFileStatus(path)
//    print(status.getPath)


    for( i <- 0 until 2){
      val temp = new temp()
      temp.start()
    }
    Thread.sleep(100000)
  }

  class temp extends Thread{

   override def run(): Unit ={
     val conf = new NetFlowConf()
     conf.load("E:\\project\\netflow\\conf\\netflow-defaults.conf.template")

     val record: FileWriter = new FileWriter("e://result "+ Thread.currentThread().getId +" .txt")
     val analysis = new AnalysisFlowData(conf)
     analysis.setTestwriter(record)

     for( i <- 0 until  1){
       val in = new FileInputStream("e://netflow.pcap")
       val data = new DataInputStream(in)

       val start = System.currentTimeMillis()
       analysis.analysisStream( data )
       record.write( "[ total time ] the result is : " + String.valueOf(System.currentTimeMillis() - start) + System.getProperty("line.separator"))
       in.close()
     }
      analysis.closeWriter()
   //  analysis.closeDic()
     record.flush()
     record.close()
    }
  }
}
