package cn.ac.ict.acs.netflow.load.worker.bgp

object BGPRoutingTable {

  type DST_ADDR = (Array[Byte], Array[Byte])

  def search(dst_addr: DST_ADDR): BGPTuple = ???

  def update(tuple: BGPTuple): Unit = ???

}

case class BGPTuple(fields: Array[Any])
