package cn.ac.ict.acs.netflow.metrics.sink

trait Sink {
  def start(): Unit
  def stop(): Unit
  def report(): Unit
}
