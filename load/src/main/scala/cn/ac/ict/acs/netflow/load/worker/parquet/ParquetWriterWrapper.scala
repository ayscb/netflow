package cn.ac.ict.acs.netflow.load.worker.parquet

import cn.ac.ict.acs.netflow.load.worker.{Row, Writer, WriterWrapper}

class ParquetWriterWrapper extends WriterWrapper {

  var w1: Writer = _
  var w2: Writer = _

  def init() = ???

  def write(rowIter: Iterator[Row], packetTime: Long) = ???

  def close() = ???
}