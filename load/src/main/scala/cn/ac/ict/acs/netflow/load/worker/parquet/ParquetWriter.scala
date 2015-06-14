package cn.ac.ict.acs.netflow.load.worker.parquet

import java.util.concurrent.atomic.AtomicInteger

import cn.ac.ict.acs.netflow.load.util.MemoryManager
import org.apache.hadoop.fs.Path
import parquet.column.ParquetProperties.WriterVersion
import parquet.hadoop.ParquetWriter
import parquet.hadoop.metadata.CompressionCodecName

import cn.ac.ict.acs.netflow.NetFlowConf
import cn.ac.ict.acs.netflow.load.LoadConf
import cn.ac.ict.acs.netflow.load.worker.{Row, Writer}
import cn.ac.ict.acs.netflow.util.{Utils, TimeUtils}


class TimelyParquetWriter(timeInMillis: Long, val conf: NetFlowConf) extends Writer {
  import TimelyParquetWriter._

  val compression = CompressionCodecName.fromConf(
    conf.get(LoadConf.COMPRESSION, CompressionCodecName.UNCOMPRESSED.name()))
  val blockSize =
    conf.getInt(LoadConf.BLOCK_SIZE, ParquetWriter.DEFAULT_BLOCK_SIZE)
  val pageSize =
    conf.getInt(LoadConf.PAGE_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE)
  val enableDictionary =
    conf.getBoolean(LoadConf.ENABLE_DICTIONARY, ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED)
  val dictionaryPageSize =
    conf.getInt(LoadConf.DICTIONARY_PAGE_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE)
  val validating =
    conf.getBoolean(LoadConf.PAGE_SIZE, ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED)
  val writerVersion = WriterVersion.fromString(
    conf.get(LoadConf.WRITER_VERSION, WriterVersion.PARQUET_1_0.toString))
  val memoryManageEnable =
    conf.getBoolean(LoadConf.ENABLE_MEMORYMANAGER, false)

  val pw = new ParquetWriter[Row](getFilePath(timeInMillis, conf),
    new RowWriteSupport, compression, blockSize, pageSize,
    dictionaryPageSize, enableDictionary, validating,
    writerVersion, conf.hadoopConfiguration)

  def init() = {

  }

  def write(rowIter: Iterator[Row]) = ???

  def close() = ???

}

object TimelyParquetWriter {
  val fileId = new AtomicInteger(0)

  var memoryManager: MemoryManager = _

  def getFilePath(time: Long, conf: NetFlowConf): Path = {
    val basePathTime = TimeUtils.getTimeBasePathBySeconds(time, conf)
    val curFileId = fileId.getAndIncrement()
    val numStr = if (curFileId < 10) "0" + curFileId.toString else curFileId.toString
    val fileName = Utils.localHostName() + "-" + numStr + ".parquet"

    new Path(new Path(basePathTime, LoadConf.TEMP_DICTIONARY), fileName)
  }

  def registerInMemManager(pw: ParquetWriter) = {
    if (memoryManager == null) {
      synchronized {
      //  val load =
      }
    }
  }
}