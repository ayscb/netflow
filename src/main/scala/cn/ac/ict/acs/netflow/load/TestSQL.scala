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
package cn.ac.ict.acs.netflow.load

import cn.ac.ict.acs.netflow.NetFlowConf
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import parquet.io.api.Binary

import scala.reflect.internal.util.TableDef.Column

/**
 * for SQL test
 *
 * Created by ayscb on 2015/4/1.
 */
object TestSQL {

  def main(args: Array[String]) {

    Template.load(args(0))
    val netFlowConf = new NetFlowConf().load(args(1))

    val sparkConf = new SparkConf().setAppName("NF_dataText")
    val sc = new SparkContext(sparkConf)
    val sqlConetxt = new SQLContext(sc)
    Template.load(args(0))
    sqlConetxt.sparkContext.hadoopConfiguration.set("fs.defaultFS", netFlowConf.dfsName)
    val df = sqlConetxt.parquetFile(netFlowConf.dfsName + netFlowConf.loadPath)

    df.printSchema()
    df.registerTempTable("par")
    val result = sqlConetxt.sql(
      "select flow_time,ipv4_src_addr,l4_src_port,ipv4_dst_addr,l4_dst_port " +
        "from par where ipv4_src_addr < 200")
    result.map(t =>
      " time: " + t(0) +
      " ipv4_src_addr: " + show(t(1).asInstanceOf[Array[Byte]]) +
      " port" + t(2) +
      " ipv4_dst_addr: " + show(t(3).asInstanceOf[Array[Byte]]) +
      " port: " + t(4))
      .take(10).foreach(println)


  }

  //show(t(1).asInstanceOf[])
  def show(data: Array[Byte]): String = {
    var result: String = ""
    data.foreach(x => result += (x & 0xFF).toString + ".")
    result
  }
}
