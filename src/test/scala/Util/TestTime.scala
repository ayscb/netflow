package Util

import cn.ac.ict.acs.netflow.{TimeUtil, NetFlowConf}
import org.scalatest.tags._
/**
 * Created by ayscb on 2015/4/13.
 */
@CPU
class TestTime extends org.scalatest.FunSuite{
475992

  test(" check the TimeUtil "){
    val conf = new NetFlowConf()
    val dateStr = "2015-02-21:22:12"
    val sec = TimeUtil.timeToSeconds(conf,dateStr)
    val dateStr2 = TimeUtil.secnodsToTime(conf,sec)
    assert(dateStr2 == dateStr)
  }


  test(" check the date"){

  }

  def main(args: Array[String]) {
    val conf = new NetFlowConf()
    val path = TimeUtil.getCurrentTimePath(conf)
    println(path)
  }
}
