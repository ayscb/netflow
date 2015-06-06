package cn.ac.ict.acs.netflow.metrics.sink

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.codahale.metrics.{Slf4jReporter, MetricRegistry}

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
