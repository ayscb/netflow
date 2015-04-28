//package cn.ac.ict.acs.netflow.load2.netFlow
//
//import scala.collection.immutable.HashMap
//
///**
// * Created by ayscb on 2015/4/14.
// */
//
////object AnalysisV9 {
////  val templates = new HashMap[Int,Template]
////}
//
//class AnalysisV9_1() extends NetFlowVersion{
//
//  //--------v9 header -------------
//  val version = 9         // 16 bit
//  var flowSetCount = 0    // 16 bit
//  var systemUptime = 0L    // 32 bit
//  var unixSeconds = 0L     // 32 bit
//  var packageSequence = 0L // 32 bit
//  var sourceID = 0L        // 32 bit
//  // ------------------------------
//  val headLength = 20   // 20Byte
//
//  override def unPackHeader(data: Array[Byte], info: (Int, Int)): Unit = {
//    var offset = info._1 + 2  // skip version
//    flowSetCount = BytesUtil.toUShort( data, offset)
//    offset += 2
//    systemUptime = BytesUtil.toUInt( data, offset)
//    offset += 4
//    unixSeconds = BytesUtil.toUInt( data, offset)
//    offset += 4
//    packageSequence = BytesUtil.toUInt( data, offset)
//    offset += 4
//    sourceID = BytesUtil.toUInt( data, offset)
//  }
//
//  override def unPackBody(data: Array[Byte], info: (Int, Int)): Unit = {
//    var bodyOffset = headLength
//
//    // deal with all flowset
//    for( i <- 0 until flowSetCount){
//      val flowSet = BytesUtil.toUShort(data,bodyOffset)
//      if( flowSet < 256){
//        bodyOffset = updateTemplates(data,bodyOffset)
//      }else {
//        bodyOffset = analysisData(data, bodyOffset)
//      }
//    }
//  }
//
//  private def updateTemplates( data: Array[Byte], offset :Int ): Int ={
//    var tmpOffset = offset + 2  // skip flowSetID (2Byte)
//    val flowLen = BytesUtil.toUShort(data,tmpOffset)
//    tmpOffset += 2
//    while( flowLen != (tmpOffset - offset) ){
//      val tmpId = BytesUtil.toUShort(data,tmpOffset)
//      tmpOffset += 2
//      val fieldCount = BytesUtil.toUShort(data,tmpOffset)
//      tmpOffset += 2
//      val template = AnalysisV9_1.templates
//        .getOrElse(tmpId,new Template(tmpId,fieldCount))
//      template.updateTemplate(tmpId,data,tmpOffset)
//    }
//    offset + flowLen
//  }
//
//  private def analysisData( data: Array[Byte], offset :Int ): Int ={
//    0
//  }
//}
//
//class AnalysisV5_1 extends NetFlowVersion {
//  override def unPackHeader(data: Array[Byte], info: (Int, Int)): Unit = {
//
//  }
//
//  // info for whole netflow data ( offset, length )
//  override def unPackBody(data: Array[Byte], info: (Int, Int)): Unit = {
//
//  }
//}
//
//
