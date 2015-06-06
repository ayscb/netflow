package cn.ac.ict.acs.netflow.load.master

import com.codahale.metrics.{Gauge, MetricRegistry}

import cn.ac.ict.acs.netflow.metrics.source.Source

class LoadMasterSource(val master: LoadMaster) extends Source {
  override val metricRegistry = new MetricRegistry()
  override val sourceName = "loadmaster"

  import MetricRegistry._

  metricRegistry.register(name("aliveLoadWorkers"), new Gauge[Int] {
    override def getValue: Int = master.workers.filter(_.state == WorkerState.ALIVE).size
  })

  // TODO
  metricRegistry.register(name("aliveReceivers"), new Gauge[Int] {
    override def getValue: Int = master.receiverToSocket.keySet.size
  })
}
