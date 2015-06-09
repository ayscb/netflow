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
package cn.ac.ict.acs.netflow.metrics.sink

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.codahale.metrics.{ Slf4jReporter, MetricRegistry }

import cn.ac.ict.acs.netflow.metrics.MetricsSystem
import cn.ac.ict.acs.netflow.util.TimeUtils

class Slf4jSink(
    val property: Properties,
    val registry: MetricRegistry) extends Sink {

  val SLF4J_DEFAULT_PERIOD = 10L

  val SLF4J_KEY_PERIOD = "period"

  val pollPeriod = Option(property.getProperty(SLF4J_KEY_PERIOD)) match {
    case Some(s) => TimeUtils.timeStringAsSec(s)
    case None => SLF4J_DEFAULT_PERIOD
  }

  MetricsSystem.checkMinimalPollingPeriod(TimeUnit.SECONDS, pollPeriod)

  val reporter: Slf4jReporter = Slf4jReporter.forRegistry(registry)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .convertRatesTo(TimeUnit.SECONDS)
    .build()

  override def start() {
    reporter.start(pollPeriod, TimeUnit.SECONDS)
  }

  override def stop() {
    reporter.stop()
  }

  override def report() {
    reporter.report()
  }
}
