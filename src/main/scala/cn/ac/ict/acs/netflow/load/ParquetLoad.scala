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

    val template = TemplateV2.load(args(0))
    val netFlowConf = new NetFlowConf().load(args(1))

    val sparkConf = new SparkConf().setAppName("NF_dataLoad" + (if(args.size==6) args(5) else " ") )
    sparkConf.set("spark.sql.parquet.compression.codec",args(2))

    val sc = new SparkContext(sparkConf)
    sc.hadoopConfiguration.set("fs.defaultFS", netFlowConf.dfsName)
    sc.hadoopConfiguration.set("parquet.block.size",args(3))
    val sqlContext = new SQLContext(sc)

    val dataRDD: DataRDD = new DataRDD(
      netFlowConf.loadStartInSec,
      netFlowConf.loadEndInSec,
      netFlowConf.loadInterval,
      netFlowConf.loadRate,
      template,
      sc)

    val rr = dataRDD.asInstanceOf[RDD[Row]]
    val df = sqlContext.createDataFrame(rr, tableSchema_back)
    if( args(4).equalsIgnoreCase("true"))
      df.count()
    else{
      val path =
        if( args.size == 6)
          "/" + args(5)
      else
          netFlowConf.loadPath
      df.saveAsParquetFile(path)
    }
    sc.stop()
  }
}

