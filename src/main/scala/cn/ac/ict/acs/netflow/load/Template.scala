package cn.ac.ict.acs.netflow.load

import java.lang.ThreadLocal

import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import scala.collection.mutable
import scala.collection.mutable.Map
import scala.io.Source
import scala.util.Random

/**
 * template about the netflow  V9 data
 * Created by ayscb on 2015/3/29.
 */

case class Template(intervalSecond: Int, dataRate: Int, template: Map[Int, Int]) {

  @transient var currTime = 0L
  @transient var bytesCount = 0L          // 通过与速率比较决定 秒数 + 1
  @transient var TotalBytesCount = 0L     // for test
  @transient var rd: Random = new Random()

  val variableKey = Set(1, 2, 3, 10, 14, 16, 17, 19, 20, 23, 24, 40, 41, 42, 82, 83, 84, 85, 86, 94, 100)

  def getBytes(): Long = TotalBytesCount

  def getRowData(startTime: Long, row: GenericMutableRow): Boolean = {
    rd = if (rd == null) new Random() else rd
    if (template.size == 0) throw new java.lang.RuntimeException("should call praseCommand first")
    if (currTime < startTime) currTime = startTime
    if (currTime > startTime + intervalSecond) return false

    row.setLong(0, currTime) // time

    for (key <- template.keySet) {
      key match {
        case (8 | 12 | 15) => row.update(key, getIPV4)
        case (27 | 28 | 62 | 63) => row.update(key, getIPV6)
        case (56 | 57 | 80 | 81) => row.update(key, getMAC)
        case _ =>
          val valLen = template.getOrElse(key, -1)
          getSampleData(key, valLen, row)
      }
    }

    // update the byte count
    bytesCount += ( 8 + Template.rowLength )

    if (bytesCount > dataRate) {
      TotalBytesCount += bytesCount
      bytesCount = 0
      currTime = currTime + 1
    }
    true
  }

  private def getSampleData(key: Int, valueLen: Int, row: GenericMutableRow): Unit = {
    if (variableKey.contains(key)) {
      row.update(key, getStringDataLength(valueLen))
    } else {
      valueLen match {
        case 1 =>
          val v = rd.nextInt(Template.BYTE_MAX).toShort
          row.setShort(key, v)

        case 2 =>
          val v = rd.nextInt(Template.SHORT_MAX)
          row.setInt(key, v)

        case 4 =>
          val v = Math.abs(rd.nextLong() % Template.INT_MAX)
          row.setLong(key, v)

        case _ =>
          row.update(key, getStringDataLength(valueLen))
      }
    }
  }

 private def getStringDataLength(dataLen: Int): Array[Byte] = {
    val value = new Array[Byte](dataLen)
    for (i <- 0 until dataLen) rd.nextBytes(value)
    value
  }

  private def getIPV4(): Array[Byte] = getStringDataLength(4)

  // 16个字节 分8组 (  FE80:0000:0000:0000:AAAA:0000:00C2:0002 )
  private def getIPV6(): Array[Byte] = getStringDataLength(16)

  // 6个字节 表示的是  00-23-5A-15-99-42
  private def getMAC(): Array[Byte] = getStringDataLength(6)
}

object Template {

  val BYTE_MAX = 256
  val SHORT_MAX = 65536
  val INT_MAX = 4294967296L
  val IP_MAX = 256

  val DHM_TIME = "yyyy-MM-dd|HH:mm"
  val dhmFormat = DateTimeFormat.forPattern(DHM_TIME)

  // configure
  /** 要生成总数据的开始时间，比如2015-01-01 05:02 **/
  private var StartTimeInSeconds: Long = 0

  /** 要生成总数据的结束时间，比如2015-01-01 05:02 **/
  private var EndTimeInSeconds: Long = 0

  /** 最后一层文件的时间跨度，单位是秒 **/
  private var intervalSecond: Int = 60

  /** 生成数据量 的速率 bytes/s **/
  private var dataRate = 1 // 200 MB/s

  /** 存储在HDFS上的根目录名 **/
  private var rootPath: String = "/netParquet"

  /** HDFS的地址 **/
  private var hdfsPath: String = ""

  /** 一行数据的长度**/
  private var rowLength  = 0

  private val template: mutable.Map[Int, Int] = new mutable.HashMap[Int, Int]


  def getInterValSecond = intervalSecond
  def getHDFSAdderss =  hdfsPath
  def getRootPath = rootPath

  /** 获取总开始时间（单位 秒） **/
  def getStartTime = StartTimeInSeconds

  /** 获取总结束时间（单位 秒） **/
  def getEndTime =  EndTimeInSeconds


  /**
   * template
   *
   * @param fileName template  file path
   */
  def loadTemplateFromFile(fileName: String): Template = {

    val file = Source.fromFile(fileName)
    if (file == null)
      throw new java.lang.IllegalAccessError(String.format("the file %s does not exist!", fileName))

    val lineIter = file.getLines()
    for (line <- lineIter) {
      val kv: Array[String] = line.split(" ")

      if (kv(0).startsWith("set")) {
        parseCommand(kv(1))
      } else {
        val valueLen = kv(1).toInt
        template += (kv(0).toInt -> valueLen)
        rowLength += valueLen
      }
    }
    file.close()
    Template(intervalSecond, dataRate, template)
  }

  private def parseCommand(lineStr: String) = {
    val cmds = lineStr.trim.split("=")
    val key = cmds(0).trim.toLowerCase
    key match {
      case "datarate" => dataRate = cmds(1).trim.toInt * 1024 * 1024 //1MB base
      case "intervalminute" => intervalSecond = cmds(1).trim.toInt
      case "hdfspath" => hdfsPath = cmds(1).trim
      case "startday" =>
        StartTimeInSeconds = DateTime.parse(cmds(1), dhmFormat).getMillis / 1000
      case "endday" =>
        EndTimeInSeconds = DateTime.parse(cmds(1), dhmFormat).getMillis / 1000
      case "rootpath" => rootPath = {
        if (cmds(1).trim.startsWith("/"))
          cmds(1).trim
        else
          "/".concat(cmds(1).trim)
      }
    }
  }
}
