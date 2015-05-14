package cn.ac.ict.acs.netflow.load2

import java.io.{DataInputStream, FileInputStream, FileWriter}
import java.nio.ByteBuffer

import cn.ac.ict.acs.netflow.NetFlowConf
import cn.ac.ict.acs.netflow.load2.netFlow.AnalysisFlowData

/**
 * Created by ayscb on 2015/5/12.
 */
object load {
  val buffer = ByteBuffer.allocate(1500)

  val conf = new NetFlowConf()
  conf.load("E:\\project\\netflow\\conf\\netflow-defaults.conf.template")

  val thread = new Thread(){

      val record: FileWriter = new FileWriter("e://result "+ Thread.currentThread().getId +" .txt")
      val analysis = new AnalysisFlowData(conf)
      analysis.setTestwriter(record)

      for( i <- 0 until  1){
        val in = new FileInputStream("e://netflow.pcap")
        val data = new DataInputStream(in)

        val start = System.currentTimeMillis()
        analysis.analysisStream( data )
        record.write( "[ total time ] the result is : " + String.valueOf(System.currentTimeMillis() - start) + System.getProperty("line.separator"))
        in.close()
      }
      analysis.closeWriter()
      record.flush()
      record.close()
    }

  def main(args: Array[String]) {

    for( i <- 0 until 1){
      thread.start()
    }
    Thread.sleep(100000)
  }
}
