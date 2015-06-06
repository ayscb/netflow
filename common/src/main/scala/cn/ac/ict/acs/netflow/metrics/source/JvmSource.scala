package cn.ac.ict.acs.netflow.metrics.source

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.jvm.{MemoryUsageGaugeSet, GarbageCollectorMetricSet}

class JvmSource extends Source {
  override val sourceName = "jvm"
  override val metricRegistry = new MetricRegistry()

  metricRegistry.registerAll(new GarbageCollectorMetricSet)
  metricRegistry.registerAll(new MemoryUsageGaugeSet)
}
