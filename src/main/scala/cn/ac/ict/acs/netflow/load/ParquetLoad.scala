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
    if(args.length !=1) throw new RuntimeException("arg should not be null")
    val template = Template.loadTemplateFromFile(args(0))

    val sparkConf = new SparkConf().setAppName("NF_dataLoad")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    sqlContext.sparkContext.hadoopConfiguration.set("fs.defaultFS", Template.getHDFSAdderss)

    val dataRDD: DataRDD = new DataRDD(Template.getStartTime,
      Template.getEndTime, Template.getInterValSecond, template, sc)

    val rr = dataRDD.asInstanceOf[RDD[Row]]

    val df = sqlContext.createDataFrame(rr, nfSchema)

    df.saveAsParquetFile(Template.getRootPath)
    sc.stop()
  }
}

