package cn.ac.ict.acs.netflow.sql

import cn.ac.ict.acs.netflow.{TimeUtil, IPv4, NetFlowConf}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

object GBFTest {

  /**
   * Select ipv4_dest, count(ipv4_dest)
   *  from t
   *  where ipv4 = `ip` && time in range
   *  group by ipv4_dest;
   *
   * @param args arg(0) - parquetFolderPath
   *        arg(1) - ip to be used in filter
   *        arg(2) - start time - yyyy-MM-dd:HH:mm
   *        arg(3) - end time - yyyy-MM-dd:HH:mm
   */
  def main(args: Array[String]) {

    val nfConf = new NetFlowConf

    val sparkConf = new SparkConf().setAppName("NF_GROUPBY")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val df: DataFrame = sqlContext.parquetFile(args(0).split(","): _*)

    val filterIp = IPv4.str2Bytes(args(1))
    val startTime = TimeUtil.timeToSeconds(nfConf, args(2))
    val endTime = TimeUtil.timeToSeconds(nfConf, args(3))

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
        + IPv4.bytes2String(row.getAs[Array[Byte]](0))
        + " : " + row.get(1)
        + " ]")
    })

    sc.stop()
  }

}
