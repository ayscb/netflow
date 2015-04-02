package cn.ac.ict.acs.netflow.load

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import parquet.io.api.Binary

import scala.reflect.internal.util.TableDef.Column

/**
 * for SQL test
 *
 * Created by ayscb on 2015/4/1.
 */
object TestSQL {

  def main(args: Array[String]) {

    Template.loadTemplateFromFile(args(0))
    Config.getConfigure(args(1))

    val sparkConf = new SparkConf().setAppName("NF_dataText")
    val sc = new SparkContext(sparkConf)
    val sqlConetxt = new SQLContext(sc)
    Template.loadTemplateFromFile(args(0))
    sqlConetxt.sparkContext.hadoopConfiguration.set("fs.defaultFS", Config.getHDFSAdderss)
    val df = sqlConetxt.parquetFile(Config.getHDFSAdderss + Config.getRootPath)

    df.printSchema()
    df.registerTempTable("par")
    val result = sqlConetxt.sql("select flow_time,ipv4_src_addr,l4_src_port,ipv4_dst_addr,l4_dst_port from par where ipv4_src_addr < 200")
    result.map(t =>"time: " + t(0) + " ipv4_src_addr: " + show ( t(1).asInstanceOf[Array[Byte]] ) + " port" + t(2)
      + "  ipv4_dst_addr: " + show ( t(3).asInstanceOf[Array[Byte]] ) + " port: " + t(4) )
      .take(10).foreach(println)


  }
//show(t(1).asInstanceOf[])
  def show(data :Array[Byte]) : String = {
  var result : String = ""
  data.foreach(x=> result += (x & 0xFF).toString + ".")
  result
  }
}
