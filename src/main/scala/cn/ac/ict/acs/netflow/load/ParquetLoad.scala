package cn.ac.ict.acs.netflow.load

import cn.ac.ict.acs.netflow.{NetFlowSchema, NetFlowConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}


object ParquetLoad {

  import NetFlowSchema._

  def main(args: Array[String]) {
    if (args.length != 2) throw new RuntimeException("arg should not be null")

    val template = Template.load(args(0))
    val netFlowConf = new NetFlowConf().load(args(1))

    val sparkConf = new SparkConf().setAppName("NF_dataLoad")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    sqlContext.sparkContext.hadoopConfiguration.set("fs.defaultFS", netFlowConf.dfsName)

    val dataRDD: DataRDD = new DataRDD(
      netFlowConf.loadStartInSec,
      netFlowConf.loadEndInSec,
      netFlowConf.loadInterval,
      netFlowConf.loadRate,
      template,
      sc)

    val rr = dataRDD.asInstanceOf[RDD[Row]]
    val df = sqlContext.createDataFrame(rr, tableSchema)
    // val hdfsPath = Config.getHDFSAdderss + Config.getRootPath
    // df.rdd.saveAsTextFile(hdfsPath)
    df.saveAsParquetFile(netFlowConf.loadPath)
    sc.stop()
  }
}

