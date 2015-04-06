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

abstract class Query {
  def name: String

  def sql: String

  def udfs: Seq[UDF]

  def frequency: Interval

  def output: OutputDesc
}

case class AdhocQuery(name: String, sql: String, udfs: Seq[UDF]) extends Query {
  override def frequency = Interval.once()

  def output = ???
}

case class ReportQuery(
  name: String,
  sql: String,
  udfs: Seq[UDF],
  frequency: Interval) extends Query {
  require(!frequency.once)

  def output = ???
}

case class OnlineQuery(
  name: String,
  sql: String,
  udfs: Seq[UDF],
  frequency: Interval) extends Query {
  require(!frequency.once)

  def output = ???
}

abstract class UDF {
  def name: String
}

case class PredefinedFunction(name: String, auxData: Seq[Any]) extends UDF

case class NewFunction(name: String, func: Any) extends UDF


/**
 * @param time Interval in seconds
 */
case class Interval(time: Long) {
  require(time >= 0)

  def once = time == 0
}

object Interval {

  val _minute = 60
  val _hour = _minute * 60
  val _day = _hour * 24
  val _week = _day * 7

  def minutes(m: Int) = new Interval(_minute * m)

  def hours(h: Int) = new Interval(_hour * h)

  def days(d: Int) = new Interval(_day * d)

  def weeks(w: Int) = new Interval(_week * w)

  def once() = new Interval(0)

}
