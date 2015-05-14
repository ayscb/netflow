package cn.ac.ict.acs.netflow.load2.parquetUtil

import java.net.InetAddress
import java.util.concurrent.atomic.AtomicInteger

import cn.ac.ict.acs.netflow.load2.NetFlowArgument
import cn.ac.ict.acs.netflow.{Logging, TimeUtil, NetFlowConf}
import org.apache.hadoop.fs.Path
import parquet.column.ParquetProperties.WriterVersion
import parquet.hadoop.ParquetWriter
import parquet.hadoop.metadata.CompressionCodecName

/**
 * get the parquet writer from some configure
 * Created by ayscb on 2015/4/18.
 */
object NetFlowWriterUtil {

  private var maxLoad: Float = 0.0f
  private var  memoryManager : MemoryManager = _

  private def initMemoryManager( netFlowConf: NetFlowConf ) : Unit = synchronized {
    if (memoryManager == null) {
      val maxLoad =
        netFlowConf.getFloat(
          NetFlowArgument.MEMORY_POOL_RATIO,
          MemoryManager.DEFAULT_MEMORY_POOL_RATIO)
      val minAllocation =
        netFlowConf.getLong(
          NetFlowArgument.MIN_MEMORY_ALLOCATION,
          MemoryManager.DEFAULT_MIN_MEMORY_ALLOCATION)
      memoryManager =
        new MemoryManager(maxLoad, minAllocation, writeSupport.getSchema)
    }
  }

  private val host = InetAddress.getLocalHost.getHostName

  //*************************************************************************************
  private lazy val writeSupport = new DataFlowSingleWriteSupport()

  def apply(netflowConf : NetFlowConf) = new NetFlowWriterUtil( netflowConf )

  private val fileIdx = new AtomicInteger()
  //************************** other method ********************************************

  def main(args: Array[String]) {
    val netflow = new NetFlowWriterUtil(new NetFlowConf())
  }
}

class NetFlowWriterUtil( val netflowConf : NetFlowConf ) extends Logging {

  lazy val compress =
    CompressionCodecName.fromConf(
      netflowConf.get(
        NetFlowArgument.COMPRESSION,
        CompressionCodecName.UNCOMPRESSED.name()))
  lazy val blockSize =
    netflowConf.getInt(NetFlowArgument.BLOCK_SIZE,ParquetWriter.DEFAULT_BLOCK_SIZE)
  lazy val pageSize =
    netflowConf.getInt(NetFlowArgument.PAGE_SIZE,ParquetWriter.DEFAULT_PAGE_SIZE)
  lazy val enableDictionary =
    netflowConf.getBoolean(NetFlowArgument.ENABLE_DICTIONARY,ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED)
  lazy val dictionaryPageSize =
    netflowConf.getInt(NetFlowArgument.DICTIONARY_PAGE_SIZE,ParquetWriter.DEFAULT_PAGE_SIZE)
  lazy val vilidating =
    netflowConf.getBoolean(NetFlowArgument.PAGE_SIZE,ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED)
  lazy val writerVersion =
    WriterVersion.fromString(netflowConf.get(NetFlowArgument.WRITER_VERSION, WriterVersion.PARQUET_1_0.toString))

  lazy val MemoryManageEnable =
    netflowConf.getBoolean(NetFlowArgument.ENABLE_MEMORYMANAGER, defaultValue = false)

  private var writer : ParquetWriter[NetflowGroup] = null
  private var nextTime : Long = 0L
  private var workPath :Path = _

  private def initParquetWriter( file : Path  ) : Unit = {
    log.info(s"[ NetFlow : Parquet block size(MB) : %d MB ] ".format( blockSize / 1024 /1024 ) )
    log.info(s"[ NetFlow : Parquet page size(KB) : %d KB ] ".format( pageSize / 1024) )
    log.info(s"[ NetFlow : Parquet dictionary page size :  $dictionaryPageSize ]")
    log.info(s"[ NetFlow : Dictionary is $enableDictionary ]")
    log.info(s"[ NetFlow : Validation is $vilidating ]")
    log.info(s"[ NetFlow : Writer version is $writerVersion ]")

    writer = new ParquetWriter[NetflowGroup](
      file,NetFlowWriterUtil.writeSupport,compress,
      blockSize,pageSize,
      dictionaryPageSize,enableDictionary,vilidating,writerVersion,netflowConf.hadoopConfigure)

    if( MemoryManageEnable ) {
      NetFlowWriterUtil.initMemoryManager(netflowConf)
      if(NetFlowWriterUtil.memoryManager.getMemoryPoolRatio != NetFlowWriterUtil.maxLoad )
        log.warn("The configuration [ %s ] has been set. It should not be reset by the new value: %f"
          .format(NetFlowArgument.MEMORY_POOL_RATIO,NetFlowWriterUtil.maxLoad))
      NetFlowWriterUtil.memoryManager.addWriter(writer,blockSize)
    }
  }

  private def initParquetWriter ( time : Long ) : Unit = {
    val basePathTime = TimeUtil.getTimeBasePathBySeconds(netflowConf,time)
    workPath = new Path(
        new Path ( basePathTime,NetFlowArgument.TEMP_DICTIONARY ),
        getworkFileStr
        )

    initParquetWriter( workPath )
    log.info(s"[ Netflow: write the file $workPath ]")
  }

  def UpdateCurrentWriter(currentTime: Long, memoryManageEnable : Boolean = false ): Unit= {
    if (currentTime > nextTime){
      nextTime = TimeUtil.getNextBaseTime(netflowConf,currentTime)

      if( writer != null ) {
        closeParquetWriter()
        initParquetWriter( nextTime )
        log.info(s" [ Netflow : next time : $nextTime ] ")
      }else{
        val currBaseTime = TimeUtil.getCurrentBastTime(netflowConf,currentTime)
        initParquetWriter(currBaseTime)
        log.info(s" [ Netflow : next time : $currBaseTime ] ")
      }
    }
  }

  def writeData( data :NetflowGroup ) = {
    writer.write(data)
  }

  def closeParquetWriter(): Unit ={
    writer.close()
    if( NetFlowWriterUtil.memoryManager != null )
      NetFlowWriterUtil.memoryManager.removeWriter(writer)
    writer = null

    // workPath 2015/1/1/1/00/_temp/host-01.parquet => 2015/1/1/1/00/host-01.parquet
    val fileName = workPath.getName               // fileName = host-01.parquet
    val workBase = workPath.getParent.getParent   // workBase = 2015/1/1/1/00/

    val fs = workBase.getFileSystem(netflowConf.hadoopConfigure)
    fs.rename(workPath,new Path(workBase,fileName))
    fs.close()
  }

  private def getworkFileStr: String = {
    val num = NetFlowWriterUtil.fileIdx.getAndIncrement
    val numStr = if (num < 10) "0" + num.toString else num.toString
    NetFlowWriterUtil.host + "-" + numStr + ".parquet"
  }
}
