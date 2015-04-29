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

import akka.actor.Actor

import org.apache.spark.sql.{GroupedData, DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

import cn.ac.ict.acs.netflow.{Logging, NetFlowConf}
import cn.ac.ict.acs.netflow.util.ActorLogReceive



object SQLConstruction {

  class QueryBackend(
      masterUrl: String
    ) extends Actor with ActorLogReceive with Logging {
    def receiveWithLogging = ???
  }

  def main(args: Array[String]) {
    val nfConf = new NetFlowConf

    val sparkConf = new SparkConf().setAppName("Generate_New_Query_Name")
    sparkConf.set("spark.sql.parquet.filterPushdown", "true")
    sparkConf.set("spark.sql.parquet.task.side.metadata", "false")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val df: DataFrame = sqlContext.parquetFile("hdfs://path/to/netflow/load")

    val logical = parseJsonQuery(Query(None, None, None, None), df)

    logical.saveAsParquetFile("hdfs://path/to/query/result")



    sc.stop()
  }

  def parseJsonQuery(q: Query, df: DataFrame): DataFrame = {
    val withFilter = q.filter.map(f => df.filter(f.toString)).getOrElse(df)

    val last: DataFrame = if (q.proj.isDefined) {
      withFilter.select(
        q.proj.get.projs.mkString(", "))
    } else {
      val afterGrp = q.grp match {
        case Some(g) =>
          withFilter.groupBy(g.grps.mkString(", "))
        case None =>
          withFilter.groupBy()
      }

      val afterAgg = q.agg match {
        case Some(a) =>
          afterGrp.agg(a.aggs.map {f => (f.name, f.cols.mkString(","))}.toMap[String, String])
        case None =>
          withFilter
      }

      afterAgg
    }

    last
  }
}

case class Query(
    proj: Option[Projection],
    agg: Option[Aggregation],
    grp: Option[GroupBy],
    filter: Option[Filter]
  )

case class Projection(projs: Array[Col])

abstract class Col

case class Aggregation(aggs: Array[FuncCol])

case class GroupBy(grps: Array[Col])

case class Filter(expr: FuncCol)

case class FuncCol(name: String, cols: Array[Col]) extends Col

case class OriCol(name: String) extends Col
