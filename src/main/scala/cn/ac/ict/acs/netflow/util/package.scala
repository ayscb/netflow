package cn.ac.ict.acs.netflow

import org.joda.time.DateTime

package object util {

}

object TimeUtil {
  def timeToSeconds(conf: NetFlowConf, t: String) =
    DateTime.parse(t, conf.timeFormat).getMillis / 1000
}

trait IP {
  def str2Bytes(ip: String): Array[Byte]
  def bytes2String(ip: Array[Byte]): String

  final def toInt(b: Byte): Int = b & 0xFF
}

object IPv4 extends IP {

  def str2Bytes(ip: String) = ip.split('.').map(_.toInt.toByte)

  def bytes2String(ip: Array[Byte]) = ip.map(toInt).mkString(".")
}

object IPv6 extends IP {
  def str2Bytes(ip: String) = ???

  def bytes2String(ip: Array[Byte]) = ???
}
