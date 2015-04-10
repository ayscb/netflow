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

import cn.ac.ict.acs.netflow.{TimeUtil, IPv4, NetFlowConf}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Select flow_time,ipv4_src_addr,ipv4_dst_addr,
 *  from t
 *  where ipv4 = `ip`
 *
 * Created by ayscb on 2015/4/7.
 */
object findIPTest {
  /**
   * Select flow_time,ipv4_src_addr,ipv4_dst_addr,
   *  from t
   *  where ipv4 = `ip`
   *
   * @param args arg(0) - parquetFolderPath
   *        arg(1) - ip to be used in filter
   */
  def main(args: Array[String]) {

    val nfConf = new NetFlowConf

    val sparkConf = new SparkConf().setAppName("NF_find_ip_and_time_record")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    if( args.length == 3 ){
      sc.hadoopConfiguration
        .set("fs.defaultFS", args(2))
    }

    import sqlContext.implicits._

    val df: DataFrame = sqlContext.parquetFile(args(0).split(","): _*)

    val filterIp = IPv4.str2Bytes(args(1))

    val ipWithCount = df
      .filter($"ipv4_dst_addr" === filterIp)
      .select("flow_time", "ipv4_src_addr","ipv4_dst_addr")
    ipWithCount.foreach(row => {
      println("Result: [ "
        + row(0) + " "
        + TimeUtil.secnodsToTime(row.getAs[Long](0)) + " "
        + IPv4.bytes2String(row.getAs[Array[Byte]](1)) + " "
        + IPv4.bytes2String(row.getAs[Array[Byte]](1))
        + " ]")
    })

    sc.stop()
  }
}
