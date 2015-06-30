package cn.as.ict.acs.netflow.load.worker.parquet

import cn.ac.ict.acs.netflow.{load, NetFlowConf}

/**
 * Created by ayscb on 6/29/15.
 */
object TestParquetWriterWrapper {

  val conf = new NetFlowConf()

  private val dicInterValTime = load.dirCreationInterval(conf)
  private val closeDelay = load.writerCloseDelay(conf)

  def getDelayTime(timeStamp: Long): Long = {
    dicInterValTime - (timeStamp - load.getTimeBase(timeStamp, conf) ) + closeDelay
  }

  def main(args: Array[String]) {

    println(dicInterValTime)
    println(closeDelay)
    for( i <- 0 to 10){
      val cuT = System.currentTimeMillis()
      val tm = cuT + i * 1000

      println(tm + "  ---> " + getDelayTime(tm)  + " --> " + load.getTimeBase(tm, conf))
    }
  }
}
