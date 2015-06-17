package cn.ac.ict.acs.netflow.load.worker.bgp

object BGPRoutingTable {

  type DST_ADDR = (Array[Byte], Array[Byte])

  def search(dst_addr: DST_ADDR): BGPTuple = {
    new BGPTuple(Array(
      "192.168.1.1",
      Array[Byte](1,2,3,4), null,
      Array[Byte](1,2,3,4),null,
      "as_path","community","adjacent_as","self_as"
     ))
  }

  def update(tuple: BGPTuple): Unit = ???

}

case class BGPTuple(fields: Array[Any])
