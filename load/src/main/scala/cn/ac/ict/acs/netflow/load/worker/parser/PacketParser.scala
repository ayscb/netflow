package cn.ac.ict.acs.netflow.load.worker.parser

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import cn.ac.ict.acs.netflow.NetFlowException

/**
 * Created by ayscb on 15-6-11.
 */

object PacketParser {
  val templates = new ConcurrentHashMap[TemplateKey, Template]

  /**
   *
   * @param packet
   * @return (Iterator[FlowSet] , PacketTime)
   */
  //def parse(packet: ByteBuffer): (Iterator[DataFlowSet], Long) = {
  def parse(packet: ByteBuffer): (Iterator[DataFlowSet], Long) = {

    // 1. router ip
    var curPos = 0
    val routerIp = {
      val ipLen = if (packet.get() == 4) 4 else 16
      val ip = new Array[Byte](ipLen)
      packet.get(ip)
      ip
    }

    var validFlag = true
    curPos = packet.position() // after ip, this position is netflow header pos
    val nfVersion = packet.getShort(curPos)
    val nfParser = {
      nfVersion match {
        case 5 => V5Parser
        case 9 => V9Parser
        case _ => throw new NetFlowException("unknown packet version.")
      }
    }

    val totalDataFSCount = nfParser.getFlowCount(packet, curPos)
    val nfTime = nfParser.getTime(packet, curPos)

    // 2. skip to netflow body position
    curPos = nfParser.getBodyPos(packet, curPos)

    val dataflowSet : Iterator[DataFlowSet] = new Iterator[DataFlowSet]() {

      private val curDFS : DataFlowSet = new DataFlowSet(packet)
      private var curDFSPos = curPos

        override def hasNext: Boolean = {

          // TODO we remove the condition "dataFSCount != totalDataFSCount".
          if (curDFSPos == packet.limit()) return false
          // skip the template flowset
          curDFSPos = nfParser.getNextFSPos(packet, curDFSPos, routerIp)
          if (curDFSPos == packet.limit()) return false
          true
        }

        override def next() = {
          curDFSPos = curDFS.getNextDfs(curDFSPos, routerIp)
          curDFS
        }
      }

    (dataflowSet, nfTime)
  }


}
