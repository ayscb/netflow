package cn.ac.ict.acs.netflow.load.worker

import java.nio.ByteBuffer

import cn.ac.ict.acs.netflow.load.worker.parser.Template

// time: Long, routerIpv4: Array[Byte], routerIpv6: Array[Byte]
class RowHeader(val fields: Array[Any])

abstract class Row {
  def header: RowHeader
  def bb: ByteBuffer
  def startPos: Int
  def template: Template
  final def length: Int = {
    require(template != null)
    template.rowLength
  }
}

class MutableRow(val bb: ByteBuffer, val template: Template) extends Row {
  var header: RowHeader = _
  var startPos: Int = _

  def setHeader(rowheader: RowHeader): Unit ={
    header = rowheader
  }
//  def length: Int = {
//    require(template != null)
//    template.rowLength
//  }

  /**
   * a new row appears
   * @param start
   */
  def update(start: Int): Unit = {
    this.startPos = start
  }
}

