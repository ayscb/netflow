package cn.ac.ict.acs.netflow.load

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark._
import org.apache.spark.sql.Row

/**
 * the rdd to product the data
 * Created by ayscb on 2015/3/29.
 */
class DataRDD(startTime: Long,
              endTime: Long,
              intervalSecond: Int,
              dataRate: Long,
              template: Template,
              sc: SparkContext)
  extends RDD[Row](sc, Nil) {


  override def compute(xp: Partition, context: TaskContext): Iterator[Row] = {

    val split = xp.asInstanceOf[DataPartition]
    val splitEndTime = split.splitEndTime

    var currTime = split.splitStartTime
    var totalBytes = 0L

    val arrys = template.getArrayContainer()

    new Iterator[Row] {
      val content: GenericMutableRow = new GenericMutableRow(101)

      override def hasNext: Boolean = currTime < splitEndTime

      override def next(): Row = {
        totalBytes += template.getRowLength
        if (totalBytes > dataRate) {
          currTime += 1
          totalBytes = 0
        }

        template.getRowData(currTime, content, arrys)
        content
      }
    }
  }

  override protected def getPartitions: Array[Partition] = {

    val partitionSize = Math.ceil(1.0 * (endTime - startTime) / intervalSecond).toInt

    val partitionArray = new Array[Partition](partitionSize)

    var nextT = startTime
    var endT = nextT

    for (i <- 0 until partitionSize - 1) {
      //pa(i) = new XPartition(id, i, time of this partition)
      endT = nextT + intervalSecond
      partitionArray(i) = new DataPartition(id, i, nextT, endT)
      nextT = endT
    }

    // compute the last time range
    val lastid = partitionSize - 1
    partitionArray(lastid) = new DataPartition(id, lastid, nextT, endTime)

    partitionArray
  }
}

class DataPartition(rddId: Int, idx: Int, val splitStartTime: Long, val splitEndTime: Long) extends Partition {
  override def hashCode(): Int = 41 * (41 + rddId) + idx

  override val index: Int = idx
}

