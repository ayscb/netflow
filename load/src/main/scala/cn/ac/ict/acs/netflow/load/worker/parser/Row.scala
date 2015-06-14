package cn.ac.ict.acs.netflow.load.worker.parser

import java.nio.ByteBuffer

/**
 * Created by ayscb on 15-6-11.
 */

//abstract class Row(){
//  def bb : ByteBuffer
//  def startPos : Int
//  def template : Template
//}
//
//class MultilRow() extends Row {
//  var bb: ByteBuffer = _
//  var startPos: Int = _
//  var template: Template = _
//
//  def setPackage(bb: ByteBuffer): MultilRow ={
//    this.bb = bb
//    template = null
//    startPos = 0
//    this
//  }
//
//  def setTemplate(template: Template): MultilRow ={
//    this.template = template
//    this.startPos = 0
//    this
//  }
//
//  def update(startPos: Int): Int ={
//    require(this.template != null)
//    this.startPos = startPos
//    startPos + template.rowLength
//  }
//}
