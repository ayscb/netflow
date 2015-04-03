package cn.ac.ict.acs.netflow.load

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

import scala.RuntimeException
import scala.collection.mutable.ListBuffer

case class TestData(key: Int, value: String)

object ParquetLoad {

  import NFSchema._

  def main(args: Array[String]) {
    if (args.length != 2) throw new RuntimeException("arg should not be null")

    val template = Template.loadTemplateFromFile(args(0))
    Config.getConfigure(args(1))

    val sparkConf = new SparkConf().setAppName("NF_dataLoad")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    sqlContext.sparkContext.hadoopConfiguration.set("fs.defaultFS", Config.getHDFSAdderss)

    val dataRDD: DataRDD = new DataRDD(
      Config.getStartTime,
      Config.getEndTime,
      Config.getInterValSecond,
      Config.getDataRate,
      template,
      sc)

    val rr = dataRDD.asInstanceOf[RDD[Row]]
    val df = sqlContext.createDataFrame(rr, nfSchema)
    // val hdfsPath = Config.getHDFSAdderss + Config.getRootPath
    // df.rdd.saveAsTextFile(hdfsPath)
    df.saveAsParquetFile(Config.getRootPath)
    sc.stop()
  }
}

