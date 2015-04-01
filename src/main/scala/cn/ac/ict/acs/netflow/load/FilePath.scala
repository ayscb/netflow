package cn.ac.ict.acs.netflow.load

import java.net.InetAddress
import java.util.Calendar

class FilePath(HDFS : String, rootPath :String, intervalSecond : Int){
  val cal : Calendar = Calendar.getInstance()
  val hostname = InetAddress.getLocalHost.getHostName
  val foot = ".par"


//  def getFilePathByMillMs( utc : Long) : String = {
//    cal.setTimeInMillis( utc * 1000)
//    val sb = new StringBuilder(HDFS).append("/")
//      .append(this.rootPath).append("/")
//    val y = cal.get(Calendar.YEAR)
//    val m = cal.get(Calendar.MONTH + 1)
//    val d = cal.get(Calendar.DAY_OF_MONTH)
//    val h = cal.get(Calendar.HOUR_OF_DAY)
//    val M = cal.get(Calendar.MINUTE)
//
//    val _M = (M  / (this.intervalSecond / 60) ) * (this.intervalSecond / 60)
//
//    sb.append(y).append("/")
//      .append(cp(m)).append("/")
//      .append(cp(d)).append("/")
//      .append(cp(h)).append("/")
//      .append(cp(_M)).append("/")
//      .append(this.hostname).append(System.currentTimeMillis()).append(foot)
//    sb.toString()
//  }

  def getFilePathByMillMs( utc : Long) : String = {
    cal.setTimeInMillis( utc * 1000)
    val sb = new StringBuilder(HDFS).append("/")
      .append(this.rootPath).append(foot).append("/")
    sb.toString()
  }

  private def cp( value :Int ) : String = {
    if( value <10 ) {
      "0".concat(value.toString)
    }else{
      value.toString
    }
  }
}

