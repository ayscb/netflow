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
package cn.ac.ict.acs.netflow

import java.io.{InputStreamReader, BufferedReader}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.io.Source
import scala.util.Sorting

import org.apache.spark.sql.SQLContext

case class Subnet(start: Int, end: Int, name: String) {
  def map(target: Int): String = if (target <= end) name else null
}

abstract class NetFlowUDF[T, RT] {

  def name: String

  def eval(input: T): RT

  def udfRegister(sqlContext: SQLContext): Unit
}

/**
 *
 * @param jobId
 * @param fileStr
 */
case class SubnetMapping(jobId: String, fileStr: String, hdfsUrl: String)
  extends NetFlowUDF[Array[Byte], String] {

  val name = "subnetmap"

  val (subnetDefinition, subnetStarts) = buildDefinition()

  def buildDefinition(): (Array[Subnet], Array[Int]) = {
    val result = new Array[Subnet](1000000)

    val configuration = new Configuration(false)
    configuration.set("fs.default.name", hdfsUrl)
    val fs = FileSystem.newInstance(configuration)

    // TODO: we could remove this file from HDFS once we finished reading
    val path = new Path(s"/netflow_tmp/$jobId/$fileStr")
    val source = Source.fromInputStream(fs.open(path), "UTF-8")
    val lines = source.getLines()
    var ipSubnetIndex = 0

    for (line <- lines) {
      val eachLineSubnet = line.split(":")
      val eachSubnet = eachLineSubnet(1).split(";")
      for (each <- eachSubnet) {
        val eachNet = each.split("/")
        val ipStartInt = ipToInt(eachNet(0))
        var SubnetMask = 1
        for (m <- 1 to (32 - eachNet(1).toByte)) {
          SubnetMask *= 2
        }
        SubnetMask = SubnetMask - 1
        val ipEndInt = ipStartInt + SubnetMask
        result(ipSubnetIndex) = Subnet(ipStartInt, ipEndInt, eachLineSubnet(0))
        ipSubnetIndex += 1
      }
    }
    Sorting.quickSort(result)(new Ordering[Subnet] {
      def compare(x: Subnet, y: Subnet) = {
        x.start.compare(y.start)
      }
    })
    (result, result.map(_.start))
  }

  override def eval(input: Array[Byte]): String = {
    if (input == null || input.size != 4) return null
    val ipInt = input(0) << 24 + input(1) << 16 + input(2) << 8 + input(3)

    var left = 0
    var right = subnetStarts.length - 1
    if(ipInt < subnetStarts(left)){
      return null
    }
    if(ipInt >= subnetStarts(right)){
      return subnetDefinition(right).map(ipInt)
    }
    while (right - left > 1) {
      val middle = (left + right) >> 1
      val middleValue = subnetStarts(middle)
      if (ipInt >= middleValue) {
        left = middle
      } else {
        right = middle
      }
    }
    subnetDefinition(left).map(ipInt)
  }

  override def udfRegister(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register(name, eval _)
  }

  private[this] def ipToInt(ipString: String): Int = {
    val array = ipString.split("\\.")
    (array(0).toInt << 24) + (array(1).toInt << 16) +
      (array(2).toInt << 8) + array(3).toInt
  }
}
