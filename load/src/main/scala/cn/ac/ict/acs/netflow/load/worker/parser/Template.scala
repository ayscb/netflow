package cn.ac.ict.acs.netflow.load.worker.parser

import java.nio.ByteBuffer
import java.util

import cn.ac.ict.acs.netflow.load.util.NetFlowShema

/**
 * Created by ayscb on 15-6-11.
 */

case class TemplateKey(routerIp :Array[Byte], templateId : Int) {

  override def hashCode(): Int = {
    var hashCode :Int = 0
    routerIp.foreach(x=> hashCode += x.hashCode())
    hashCode += templateId.hashCode()
    hashCode
  }

  override def equals(obj: scala.Any): Boolean ={
    if(obj == null) return false
    if(obj.getClass != getClass) return false

    val tobj = obj.asInstanceOf[TemplateKey]
    if(tobj.routerIp.length != routerIp.length || tobj.templateId != templateId){
      return false
    }

    util.Arrays.equals(tobj.routerIp,routerIp)
  }
}


class Template(val tmpId: Int, val fieldsCount: Int) {

  var rowLength = 0
  val keyList = new Array[Int](fieldsCount)
  val valueList = new Array[Int](fieldsCount)

  /**
   * update the template, for( v9 )
   * @param data template data
   */
  def updateTemplate(data: ByteBuffer): Template = {

    for (i <- 0 until fieldsCount){
      val key = NetFlowShema.mapKey2Clm(data.getShort)
      if (key != -1) {
        val valueLen = data.getShort
        keyList(i) = key
        valueList(i) = valueLen
        rowLength += valueLen
      }
    }
    this
  }

  /**
   * create a single template for v5 v7
   * @param key_value
   */
  def createTemplate(key_value :(Int,Int)*): Unit ={
    assert(key_value.length == fieldsCount)
    var i=0
    key_value.foreach(x=>{
      keyList(i) = NetFlowShema.mapKey2Clm(x._1)
      valueList(i) = x._2
      i += 1
    })
  }
}
