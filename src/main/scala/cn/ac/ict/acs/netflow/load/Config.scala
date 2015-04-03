package cn.ac.ict.acs.netflow.load

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.io.Source

object Config {

  private var StartTimeInSeconds: Long = 0
  // the start time we will create the file data
  private var EndTimeInSeconds: Long = 0
  // the end time we will create the file data
  private var intervalSecond: Int = 60
  // how long we write a file to disk , hdfs ....
  private var dataRate: Long = 1L
  // 200M/s
  private var rootPath: String = "/netParquet"
  private var hdfsPath: String = "/hdfs://192.168.80.110:8020"


  def getInterValSecond = intervalSecond

  def getHDFSAdderss = hdfsPath

  def getRootPath = rootPath

  def getStartTime = StartTimeInSeconds

  def getEndTime = EndTimeInSeconds

  def getDataRate = dataRate

  def main(args: Array[String]) {
    getConfigure("E:\\project\\netflow\\loadData.conf")
  }

  def getConfigure(filePath: String) = {

    val file = Source.fromFile(filePath)
    if (file == null)
      throw new java.lang.IllegalAccessError(String.format("the file %s does not exist!", filePath))

    val DHM_TIME = "yyyy-MM-dd HH:mm"
    val dhmFormat = DateTimeFormat.forPattern(DHM_TIME)

    val lineIter = file.getLines()
    for (lineStr <- lineIter if !lineStr.startsWith("#") && lineStr.trim.nonEmpty) {

      val cmds = lineStr.split("=")

      val key = cmds(0).trim.toLowerCase
      key match {
        case "datarate" => dataRate = removeSpecialFlag(cmds(1).trim).toInt * 1024 * 1024 //1MB base
        case "intervalsecond" => intervalSecond = removeSpecialFlag(cmds(1).trim).trim.toInt
        case "hdfspath" => hdfsPath = removeSpecialFlag(cmds(1).trim).trim
        case "startday" => StartTimeInSeconds = DateTime.parse(removeSpecialFlag(cmds(1).trim), dhmFormat).getMillis / 1000
        case "endday" => EndTimeInSeconds = DateTime.parse(removeSpecialFlag(cmds(1).trim), dhmFormat).getMillis / 1000
        case "rootpath" => rootPath = {
          val root = removeSpecialFlag(cmds(1).trim)
          if (root.startsWith("/"))
            root
          else
            "/".concat(root)
        }
        case _ => Unit
      }
    }
  }

  private def removeSpecialFlag(value: String): String = {
    value.replaceAll("\"", "")
  }
}
