package cn.ac.ict.acs.netflow.load

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark._
import org.apache.spark.sql.Row

/**
 * Created by ayscb on 2015/3/29.
 */
class DataRDD(startTime: Long,
              endTime: Long,
              intervalSecond: Int,
              template : Template,
              sc: SparkContext)
  extends RDD[Row](sc, Nil) {


  override def compute(xp: Partition, context: TaskContext): Iterator[Row] = {

    val splitRDD = xp.asInstanceOf[XPartition]
    val time = splitRDD.time

    println( " the template is " + template + " this start time is " + time)

    new Iterator[Row] {
      val content : GenericMutableRow = new GenericMutableRow(101)
      override def hasNext: Boolean = template.getRowData(time,content)
      override def next(): Row = content
    }
  }

  override protected def getPartitions: Array[Partition] = {
    //partition size = (endTime - startTime) * dataPerSecond / (interval * dataPersecond)
    val partitionSize = Math.ceil( 1.0 * (endTime - startTime) / intervalSecond ).toInt

    val patitionArray = new Array[Partition](partitionSize)
    for (i <- 0 until partitionSize) {
      //pa(i) = new XPartition(id, i, time of this partition)
      val nextTime: Long = startTime + i * intervalSecond
      patitionArray(i) = new XPartition(id, i, nextTime)
    }
    patitionArray
  }
}

class XPartition(rddId: Int, idx: Int, val time: Long) extends Partition {
  override def hashCode(): Int = 41 * (41 + rddId) + idx

  override val index: Int = idx
}

