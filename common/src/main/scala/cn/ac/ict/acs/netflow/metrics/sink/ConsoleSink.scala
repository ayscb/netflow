package cn.ac.ict.acs.netflow.metrics.sink

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.codahale.metrics.{ConsoleReporter, MetricRegistry}

import cn.ac.ict.acs.netflow.util.TimeUtils
import cn.ac.ict.acs.netflow.metrics.MetricsSystem

class ConsoleSink(val property: Properties, val registry: MetricRegistry,
  securityMgr: SecurityManager) extends Sink {
  val CONSOLE_DEFAULT_PERIOD = 10L

  val CONSOLE_KEY_PERIOD = "period"

  val pollPeriod = Option(property.getProperty(CONSOLE_KEY_PERIOD)) match {
    case Some(s) => TimeUtils.timeStringAsSec(s)
    case None => CONSOLE_DEFAULT_PERIOD
  }

  MetricsSystem.checkMinimalPollingPeriod(TimeUnit.SECONDS, pollPeriod)

  val reporter: ConsoleReporter = ConsoleReporter.forRegistry(registry)
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
