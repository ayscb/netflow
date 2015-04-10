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

import cn.ac.ict.acs.netflow.{NetFlowSchema, NetFlowConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}


object ParquetLoad {

  import NetFlowSchema._

  def main(args: Array[String]) {
    if (args.length != 2) throw new RuntimeException("arg should not be null")

    val template = Template.load(args(0))
    val netFlowConf = new NetFlowConf().load(args(1))

    val sparkConf = new SparkConf().setAppName("NF_dataLoad")
    val sc = new SparkContext(sparkConf)

    sc.hadoopConfiguration.set("fs.defaultFS", netFlowConf.dfsName)

    val sqlContext = new SQLContext(sc)

    val dataRDD: DataRDD = new DataRDD(
      netFlowConf.loadStartInSec,
      netFlowConf.loadEndInSec,
      netFlowConf.loadInterval,
      netFlowConf.loadRate,
      template,
      sc)

    val rr = dataRDD.asInstanceOf[RDD[Row]]
    val df = sqlContext.createDataFrame(rr, tableSchema)
    // val hdfsPath = Config.getHDFSAdderss + Config.getRootPath
    // df.rdd.saveAsTextFile(hdfsPath)
    df.saveAsParquetFile(netFlowConf.loadPath)
    sc.stop()
  }
}

