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
package cn.ac.ict.acs.netflow.sql

import cn.ac.ict.acs.netflow.{IPv4, NetFlowConf}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * select flow_time,ipv4_src_addr,l4_src_port,ipv4_dst_addr,l4_dst_port
 * from par where l4_src_port < 200
 *
 * Created by ayscb on 2015/4/7.
 */
object ShowTest {
  /**
   * select flow_time,ipv4_src_addr,l4_src_port,ipv4_dst_addr,l4_dst_port
   * from par where l4_src_port < 200
   *
   * @param args arg(0) - parquetFolderPath
   *        arg(1) - hdfs the data path in HDFS ( for test )
   */
  def main(args: Array[String]) {

    val nfConf = new NetFlowConf

    val sparkConf = new SparkConf().setAppName("NF_show_record")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    if( args.length == 2){
      sc.hadoopConfiguration.set("fs.defaultFS", args(1))
    }


    import sqlContext.implicits._

    val df: DataFrame = sqlContext.parquetFile(args(0).split(","): _*)

    df.printSchema()
    df.registerTempTable("par")
    val result = sqlContext.sql(
      "select flow_time,ipv4_src_addr,l4_src_port,ipv4_dst_addr,l4_dst_port " +
        "from par where l4_src_port < 200")

    result.foreach(row => {
      println("Result: [ "
        + row(0) + " "
        + IPv4.bytes2String(row.getAs[Array[Byte]](1)) + ":" + row.get(2) + " "
        + IPv4.bytes2String(row.getAs[Array[Byte]](3)) + ":" + row.get(4) + " "
        + " ]")
    })

    sc.stop()
  }
}
