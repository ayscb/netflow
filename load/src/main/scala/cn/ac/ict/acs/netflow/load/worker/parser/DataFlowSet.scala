package cn.ac.ict.acs.netflow.load.worker.parser

import java.nio.ByteBuffer

import cn.ac.ict.acs.netflow.load.worker.{Row, MutableRow}

/**
 * Created by ayscb on 15-6-11.
 */

/**
 * data flow set
 * ------------length(2Byte)-------------
 * |       flowsetID ( =templateID )    |  headLen_part1
 * |           flowsetLength            |  + headLen_part2 = total 4Byte
 * ----------------body------------------
 * |           record 1 field 1         |
 * |           record 1 field 2         |
 * |              .....                 |
 * |           record 1 field n         |
 * --------------------------------------
 * |           record 2 field 1         |
 * |           record 2 field 2         |
 * |              .....                 |
 * |           record 2 field n         |
 * --------------------------------------
 * |           record m field 1         |
 * |           record m field 2         |
 * |              .....                 |
 * |           record m field n         |
 * --------------------------------------
 */
class DataFlowSet(val bb: ByteBuffer) {
  private val fsHeaderLen = 4

  private var dfsStartPos = 0
  private var dfsEndPos = 0
  private var existTmp: Boolean = _

  private var template : Template = _
  private def fsId = bb.getShort(dfsStartPos)
  private def fsLen = bb.getShort(dfsStartPos + 2)
  private def fsBodyLen = fsLen - fsHeaderLen

  def getNextDfs(startPos: Int, routerIp :Array[Byte]): Int ={
    dfsStartPos = startPos
    dfsEndPos = startPos + fsLen

    val tempKey = new TemplateKey(routerIp, fsId)
    existTmp = PacketParser.templates.containsKey(tempKey)
    if(existTmp){
      template = PacketParser.templates.get(tempKey)
    }
    dfsEndPos
  }

  def getRows: Iterator[Row] = {

    new Iterator[Row] {
      var curRow = new MutableRow(bb, template)
      var curRowPos : Int = dfsStartPos + fsHeaderLen

      def hasNext: Boolean = {
        if(!existTmp) return false
        if(curRowPos == dfsEndPos) return false
        true
      }

      def next() = {
        curRow.update(curRowPos)
        curRowPos += template.rowLength
        curRow
      }
    }
  }
}

