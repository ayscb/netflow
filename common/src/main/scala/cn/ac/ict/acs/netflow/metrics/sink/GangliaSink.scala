package cn.ac.ict.acs.netflow.metrics.sink

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.ganglia.GangliaReporter
import info.ganglia.gmetric4j.gmetric.GMetric
import info.ganglia.gmetric4j.gmetric.GMetric.UDPAddressingMode

import cn.ac.ict.acs.netflow.metrics.MetricsSystem
import cn.ac.ict.acs.netflow.util.TimeUtils

class GangliaSink(val property: Properties, val registry: MetricRegistry)
    extends Sink {

  val GANGLIA_KEY_PERIOD = "period"
  val GANGLIA_DEFAULT_PERIOD = 10L

  val GANGLIA_DEFAULT_UNIT: TimeUnit = TimeUnit.SECONDS

  val GANGLIA_KEY_MODE = "mode"
  val GANGLIA_DEFAULT_MODE: UDPAddressingMode = GMetric.UDPAddressingMode.MULTICAST

  // TTL for multicast messages. If listeners are X hops away in network, must be at least X.
  val GANGLIA_KEY_TTL = "ttl"
  val GANGLIA_DEFAULT_TTL = 1

  val GANGLIA_KEY_HOST = "host"
  val GANGLIA_KEY_PORT = "port"

  def propertyToOption(prop: String): Option[String] = Option(property.getProperty(prop))

  if (!propertyToOption(GANGLIA_KEY_HOST).isDefined) {
    throw new Exception("Ganglia sink requires 'host' property.")
  }

  if (!propertyToOption(GANGLIA_KEY_PORT).isDefined) {
    throw new Exception("Ganglia sink requires 'port' property.")
  }

  val host = propertyToOption(GANGLIA_KEY_HOST).get
  val port = propertyToOption(GANGLIA_KEY_PORT).get.toInt
  val ttl = propertyToOption(GANGLIA_KEY_TTL).map(_.toInt).getOrElse(GANGLIA_DEFAULT_TTL)
  val mode: UDPAddressingMode = propertyToOption(GANGLIA_KEY_MODE)
    .map(u => GMetric.UDPAddressingMode.valueOf(u.toUpperCase)).getOrElse(GANGLIA_DEFAULT_MODE)
  val pollPeriod = propertyToOption(GANGLIA_KEY_PERIOD).map(TimeUtils.timeStringAsSec(_))
    .getOrElse(GANGLIA_DEFAULT_PERIOD)

  MetricsSystem.checkMinimalPollingPeriod(GANGLIA_DEFAULT_UNIT, pollPeriod)

  val ganglia = new GMetric(host, port, mode, ttl)
  val reporter: GangliaReporter = GangliaReporter.forRegistry(registry)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .convertRatesTo(TimeUnit.SECONDS)
    .build(ganglia)

  override def start() {
    reporter.start(pollPeriod, GANGLIA_DEFAULT_UNIT)
  }

  override def stop() {
    reporter.stop()
  }

  override def report() {
    reporter.report()
  }
}
