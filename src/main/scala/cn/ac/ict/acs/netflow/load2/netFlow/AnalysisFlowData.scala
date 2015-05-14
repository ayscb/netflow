package cn.ac.ict.acs.netflow.load2.netFlow

import java.io.{FileWriter, DataInputStream}
import java.nio.ByteBuffer
import java.util

import cn.ac.ict.acs.netflow.NetFlowConf
import cn.ac.ict.acs.netflow.load2.parquetUtil.{NetFlowWriterUtil, NetflowGroup}

/**
 * analysis the data from the bytes array
 * Created by ayscb on 2015/4/13.
 */
object AnalysisFlowData {

  // 5C F9 DD 1E 35 76 Start mac
  private val beginFlag: Array[Byte] = Array[Byte](92, -7, -35, 30, 53, 118)

  def findNextRecord(data: DataInputStream , ResuseArray : Array[Byte] , netflowBuffer : ByteBuffer ): Int = {
    val macLength = 64 // min mac length
    var find = false
    while (!find) {
      while (data.available() > macLength && data.readByte() != beginFlag(0)) {}
      if (data.available() < macLength) {
        data.close()
        return 0 // the data pack is over
      }
      ResuseArray(0) = 92
      data.read(ResuseArray, 1, 5)
      if (util.Arrays.equals(beginFlag, ResuseArray)) find = true
    }

    // find the record
    upPackMac( data, netflowBuffer )
  }

  // need to jump MAC package
  private def upPackMac(data: DataInputStream , netflowBuffer : ByteBuffer ): Int = {
    // mac format
    // ---------------------------------------
    // src MAC  : 6Byte
    // dest MAC : 6Byte
    // type     : 2Byte
    // body     : 46B ~ 1500B
    // CRC      : 4Byte
    //-----------------------------------------

    data.skip(6) // dest MAC
    val s = data.readShort() // type
    s match {
      case 0x0800 => upPackIP( data, netflowBuffer )
      case _ => -1 // unexpected
    }
  }

  // jump the Ip package
  private def upPackIP(data: DataInputStream , netflowBuffer : ByteBuffer): Int = {
    val headerLen = (data.readByte() & 0x0f) * 4 // 1Byte ( Ip version + headerLength )
    data.skipBytes(1) // 1Byte
    val ipTotalLength = data.readUnsignedShort() // 2Byte
    val ipBodyLength = ipTotalLength - headerLen
    data.skip(headerLen - 4)
    if (data.available() >= ipBodyLength) {
      data.readFully(netflowBuffer.array(), 0, ipBodyLength)
      netflowBuffer.limit(ipBodyLength)
      upPackUDP(netflowBuffer)
      1
    } else {
      -1 // unexpected
    }
  }

  private def upPackUDP(data: ByteBuffer): Unit = {
    // udp format
    // ---------------------------------------
    // src port : 16bit | dest port : 16bit
    // dupLength: 16bit | checksum  : 16bit
    // body ........
    //-----------------------------------------

    data.position(4) // skip src + desc port
 //   val udpBodyLength = data.getShort - 8 // header 8B
    data.position(8)  // skip all the udp
  }

  //*********************************************************************

  // share for all threads
  private val netflowVersions = new scala.collection.parallel.mutable.ParHashMap[Int, NetFlowAnalysis]

  private def vaildData(data: ByteBuffer): Option[NetFlowAnalysis] = synchronized {
    val version = data.getShort

    if (netflowVersions.contains(version))
      netflowVersions.get(version)
    else {
      version match {
        case 5 =>
          val v5 = new V5Analysis()
          netflowVersions +=  (5 -> v5)
          Some(v5)
        case 9 =>
          val v9 = new V9Analysis()
          netflowVersions += (9 -> v9)
          Some(v9)
        case _ => None
      }
    }
  }

  //*********************************************************************
}

class AnalysisFlowData(val netflowConf : NetFlowConf){

  // for analysis the file  ( thread !!)
  private val reuse: Array[Byte] = new Array[Byte](6)
  private val netflowDataBuffer = ByteBuffer.allocate(1500)   // receive the udp package
  private val writeUtil  = new NetFlowWriterUtil(netflowConf)    // for write

  //for test
  var writetest : FileWriter = _

  def setTestwriter( write : FileWriter ) = writetest = write

  def analysisnetflow( data : ByteBuffer): Unit = {
    val startT = System.nanoTime()

    AnalysisFlowData.vaildData(data) match {
      case Some(analysis) =>
        // dell with the netflow
        val headerData = analysis.unPackHeader(data)
        val totalFlowSet = analysis.getTotalFlowSet(headerData)
        val UnixSeconds = analysis.getUnixSeconds(headerData)
        var flowsetCount = 0

        if (totalFlowSet == 0) {
          // this package is whole templates package
          // we only update the template and skip the package
          if (analysis.isTemplateFlowSet(data)) {
            analysis.updateTemplate(data)
          }
        } else {
          while (flowsetCount != totalFlowSet && data.hasRemaining) {
            // the flow set is template , so we should undate the template
            if (analysis.isTemplateFlowSet(data)) {
              analysis.updateTemplate(data)
            } else {
              // first we should check if the template exist ( v9 : >255, not exist : -1  )
              val tmpId = analysis.isTemplateExist(data)
              if (tmpId != -1) {
                val record = new NetflowGroup(
                  analysis.getTemplate(tmpId),
                  Array( UnixSeconds ),
                  netflowDataBuffer
                )
                val startt = System.nanoTime()
                writeUtil.UpdateCurrentWriter(UnixSeconds,false)
                writeUtil.writeData(record)
                writetest.write( String.valueOf ((System.nanoTime() - startt)/1000 )+ "\t" )

                flowsetCount += record.flowCount
              }
            }
          }
        }
      case None =>
    }
    writetest.write( String.valueOf ( (System.nanoTime() - startT)/1000 )+ System.getProperty("line.separator"))
   //  writetest.write( " analysis netFlow cost time :  " + String.valueOf ( (System.nanoTime() - startT)/1000 )+ System.getProperty("line.separator"))
  }

  def analysisStream(data: DataInputStream): Unit = {
    while ( data.available() > 0 && AnalysisFlowData.findNextRecord(data,reuse,netflowDataBuffer) != 0 ) {
      analysisnetflow(netflowDataBuffer)
      data.skipBytes(4)     //  skip 4 byte mac CRC, and jump into next while cycle
    }
  }

  def closeWriter() = writeUtil.closeParquetWriter()

}
