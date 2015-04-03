package cn.ac.ict.acs.netflow.sql

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

object SQLQuery {

  val DHM_TIME = "yyyy-MM-dd:HH:mm"
  val dhmFormat = DateTimeFormat.forPattern(DHM_TIME)

  def toInt(b: Byte): Int = b & 0xFF

  def ipv4FromStr(s: String): Array[Byte] = s.split('.').map(_.toInt.toByte)

  def timeToSeconds(t: String) =
    DateTime.parse(t, dhmFormat).getMillis / 1000

  /**
   * Select ipv4_dest, count(ipv4_dest)
   *  from t
   *  where ipv4 = `ip` && time in range
   *  group by ipv4_dest;
   *
   * Usage: arg(0) - parquetFolderPath
   *        arg(1) - ip to be used in filter
   *        arg(2) - start time - yyyy-MM-dd:HH:mm
   *        arg(3) - end time - yyyy-MM-dd:HH:mm
   * @param args
   */
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("NF_GROUPBY")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val df: DataFrame = sqlContext.parquetFile(args(0))

    val filterIp = ipv4FromStr(args(1))
    val startTime = timeToSeconds(args(2))
    val endTime = timeToSeconds(args(3))

    val ipWithCount = df
      .filter($"flow_time" > startTime)
      .filter($"flow_time" < endTime)
      .filter($"ipv4_dst_addr" === filterIp)
      .select("ipv4_dst_addr")
      .groupBy("ipv4_dst_addr")
      .agg("ipv4_dst_addr" -> "count")

//    ipWithCount.explain()

    ipWithCount.foreach(row => {
      println("Result: [ "
        + row.getAs[Array[Byte]](0).map(toInt _).mkString(".")
        + " : " + row.get(1)
        + " ]")
    })

    sc.stop()
  }

}
