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
package cn.ac.ict.acs.netflow.metrics

import java.util.Properties
import java.util.concurrent.TimeUnit

import cn.ac.ict.acs.netflow.metrics.sink.Sink
import cn.ac.ict.acs.netflow.metrics.source.Source

import scala.collection.mutable

import com.codahale.metrics.MetricRegistry

import cn.ac.ict.acs.netflow.{Logging, NetFlowConf}

class MetricsSystem(
    val instance: String,
    conf: NetFlowConf) extends Logging {

  private[this] val confFile = conf.get("netflow.metrics.conf", null)
  private[this] val metricsConfig = new MetricsConfig(Option(confFile))

  private val sinks = new mutable.ArrayBuffer[Sink]
  private val sources = new mutable.ArrayBuffer[Source]
  private val registry = new MetricRegistry()

  private var running: Boolean = false

  metricsConfig.initialize()

  def start() {
    require(!running, "Attempting to start a MetricsSystem that is already running")
    running = true
    registerSources()
    registerSinks()
    sinks.foreach(_.start)
  }

  def stop() {
    if (running) {
      sinks.foreach(_.stop)
    } else {
      logWarning("Stopping a MetricsSystem that is not running")
    }
    running = false
  }

  def report() {
    sinks.foreach(_.report())
  }

  def buildRegistryName(source: Source): String = {
    val defaultName = MetricRegistry.name(source.sourceName)
    // TODO: is it enough to just use source default name?
    defaultName
  }

    def registerSource(source: Source) {
    sources += source
    try {
      val regName = buildRegistryName(source)
      registry.register(regName, source.metricRegistry)
    } catch {
      case e: IllegalArgumentException => logInfo("Metrics already registered", e)
    }
  }

  private def registerSources() {
    // find all the Source configurations for the instance
    val instConfig = metricsConfig.getInstance(instance)
    val sourceConfigs = metricsConfig.subProperties(instConfig, MetricsSystem.SOURCE_REGEX)

    // Register all the sources related to instance
    sourceConfigs.foreach { kv =>
      val classPath = kv._2.getProperty("class")
      try {
        val source = Class.forName(classPath).newInstance()
        registerSource(source.asInstanceOf[Source])
      } catch {
        case e: Exception => logError("Source class " + classPath + " cannot be instantiated", e)
      }
    }
  }

  private def registerSinks() {
    // find all the Sink configurations for the instance
    val instConfig = metricsConfig.getInstance(instance)
    val sinkConfigs = metricsConfig.subProperties(instConfig, MetricsSystem.SINK_REGEX)

    sinkConfigs.foreach { kv =>
      val classPath = kv._2.getProperty("class")
      if (null != classPath) {
        try {
          val sink = Class.forName(classPath)
            .getConstructor(classOf[Properties], classOf[MetricRegistry])
            .newInstance(kv._2, registry)
          sinks += sink.asInstanceOf[Sink]
        } catch {
          case e: Exception => {
            logError("Sink class " + classPath + " cannot be instantiated")
            throw e
          }
        }
      }
    }
  }

}

object MetricsSystem {
  val SINK_REGEX = "^sink\\.(.+)\\.(.+)".r
  val SOURCE_REGEX = "^source\\.(.+)\\.(.+)".r

  private[this] val MINIMAL_POLL_UNIT = TimeUnit.SECONDS
  private[this] val MINIMAL_POLL_PERIOD = 1

  def checkMinimalPollingPeriod(pollUnit: TimeUnit, pollPeriod: Long) {
    val period = MINIMAL_POLL_UNIT.convert(pollPeriod, pollUnit)
    if (period < MINIMAL_POLL_PERIOD) {
      throw new IllegalArgumentException("Polling period " + pollPeriod + " " + pollUnit +
        " below than minimal polling period ")
    }
  }

  def createMetricsSystem(
    instance: String, conf: NetFlowConf): MetricsSystem = {
    new MetricsSystem(instance, conf)
  }
}
