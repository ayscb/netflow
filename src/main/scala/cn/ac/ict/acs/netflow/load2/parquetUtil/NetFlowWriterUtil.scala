package cn.ac.ict.acs.netflow.load2.parquetUtil

import java.nio.ByteBuffer
import java.util
import cn.ac.ict.acs.netflow.util.Logging
import cn.ac.ict.acs.netflow.{TimeUtil, NetFlowConf}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import parquet.column.ParquetProperties
import parquet.column.ParquetProperties.WriterVersion
import parquet.hadoop.api.WriteSupport
import parquet.hadoop.metadata.CompressionCodecName
import parquet.hadoop._

import scala.collection.mutable

/**
 * get the parquet writer from some configure
 * Created by ayscb on 2015/4/18.
 */
object NetFlowWriterUtil {
  val BLOCK_SIZE: String = "parquet.block.size"
  val PAGE_SIZE: String = "parquet.page.size"
  val COMPRESSION: String = "parquet.compression"
  val ENABLE_DICTIONARY: String = "parquet.enable.dictionary"
  val DICTIONARY_PAGE_SIZE: String = "parquet.dictionary.page.size"
  val VALIDATION: String = "parquet.validation"
  val WRITER_VERSION: String = "parquet.writer.version"

  def getBlockSize( netflowConf : NetFlowConf  ) :Int =
  netflowConf.getInt(BLOCK_SIZE,ParquetWriter.DEFAULT_BLOCK_SIZE)

  def getPageSize( netflowConf : NetFlowConf  ) :Int =
    netflowConf.getInt(PAGE_SIZE,ParquetWriter.DEFAULT_PAGE_SIZE)

  def getCompress( netflowConf : NetFlowConf  ) : CompressionCodecName =
    CompressionCodecName.fromConf(netflowConf.get(COMPRESSION,CompressionCodecName.UNCOMPRESSED.name()))

  def getEnableDictionary( netflowConf : NetFlowConf ) :Boolean =
    netflowConf.getBoolean(ENABLE_DICTIONARY,ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED)

  def getDictionaryPageSize( netflowConf : NetFlowConf  ) :Int =
    netflowConf.getInt(DICTIONARY_PAGE_SIZE,ParquetWriter.DEFAULT_PAGE_SIZE)

  def getValidation( netflowConf : NetFlowConf  ) :Boolean =
    netflowConf.getBoolean(PAGE_SIZE,ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED)

  def getWriteVersion( netFlowConf: NetFlowConf )  = {
    val writeVersion = netFlowConf.get(WRITER_VERSION, WriterVersion.PARQUET_1_0.toString)
    WriterVersion.fromString(writeVersion)
  }

  //*************************************************************************************
  val ENABLE_JOB_SUMMARY: String = "parquet.enable.summary-metadata"

  def getSummaryEnable( netflowConf : NetFlowConf  ) : Boolean =
    netflowConf.getBoolean(ENABLE_JOB_SUMMARY, defaultValue = true)

  //*************************************************************************************
  val ENABLE_MEMORYMANAGER = " parquet.memory.manage.enable"

  val MEMORY_POOL_RATIO: String = "parquet.memory.pool.ratio"
  val MIN_MEMORY_ALLOCATION: String = "parquet.memory.min.chunk.size"

  def getMemoryManageEnable( netflowConf : NetFlowConf  ) : Boolean =
    netflowConf.getBoolean(ENABLE_MEMORYMANAGER, defaultValue = false)

  var maxLoad: Float = _

  private var  memoryManager : MemoryManager = _
  def initMemoryManager( netFlowConf: NetFlowConf ) : Unit = synchronized {
    if (memoryManager == null) {
      val maxLoad= netFlowConf.getFloat(MEMORY_POOL_RATIO, MemoryManager.DEFAULT_MEMORY_POOL_RATIO)
      val minAllocation = netFlowConf.getLong(MIN_MEMORY_ALLOCATION, MemoryManager.DEFAULT_MIN_MEMORY_ALLOCATION)
      memoryManager = new MemoryManager(maxLoad, minAllocation, writeSupport.getSchema)
    }
  }

  //*************************************************************************************
  private lazy val writeSupport = new DataFlowSingleWriteSupport()

  def apply(netflowConf : NetFlowConf) = new NetFlowWriterUtil( netflowConf )

  //************************** other method ********************************************
}

class NetFlowWriterUtil(netflowConf : NetFlowConf) extends Logging {

  lazy val compress = NetFlowWriterUtil.getCompress(netflowConf)
  lazy val blockSize = NetFlowWriterUtil.getBlockSize(netflowConf)
  lazy val pageSize = NetFlowWriterUtil.getPageSize(netflowConf)
  lazy val enableDictionary = NetFlowWriterUtil.getEnableDictionary(netflowConf)
  lazy val dictionaryPageSize = NetFlowWriterUtil.getDictionaryPageSize(netflowConf)
  lazy val vilidating = NetFlowWriterUtil.getValidation(netflowConf)
  lazy val writerVersion = NetFlowWriterUtil.getWriteVersion(netflowConf)

  private var writer : ParquetWriter[NetflowGroup] = null
  private var nextTime : Long = 0L

  // record the time
  private var fileQueue = new mutable.Queue[Long]()

  private def initParquetWriter( file:Path  ) : Unit = {
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

    if( NetFlowWriterUtil.getMemoryManageEnable(netflowConf) ) {
      NetFlowWriterUtil.initMemoryManager(netflowConf)
      if(NetFlowWriterUtil.memoryManager.getMemoryPoolRatio != NetFlowWriterUtil.maxLoad )
        log.warn("The configuration [ %s ] has been set. It should not be reset by the new value: %f"
          .format(NetFlowWriterUtil.MEMORY_POOL_RATIO,NetFlowWriterUtil.maxLoad))
      NetFlowWriterUtil.memoryManager.addWriter(writer,blockSize)
    }
  }

  private def initParquetWriter ( time : Long) : Unit = {
    val str = getPath(time) + "/" + System.currentTimeMillis()
    initParquetWriter( new Path (str) )
    log.info(s"[ Netflow: write the file $str ]")
  }

  private def getPath( time : Long) : String = {
    TimeUtil.getTimeBasePathBySeconds(netflowConf,time)
  }

  def UpdateCurrentWriter(currentTime: Long, memoryManageEnable : Boolean = false ): Unit= {
    if (currentTime > nextTime){
      nextTime = TimeUtil.getNextBaseTime(netflowConf,currentTime)

      if( writer != null ) {
        closeParquetWriter()
        initParquetWriter( nextTime )
        fileQueue.enqueue( nextTime )
        log.info(s" [ Netflow : next time : $nextTime ] , queue's length : $fileQueue ")
      }else{
        val currBaseTime = TimeUtil.getCurrentBastTime(netflowConf,currentTime)
        initParquetWriter(currBaseTime)
        fileQueue.enqueue(currBaseTime)
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
  }

  def closeDirectory (path : Path):Unit = {
    //mergeSummary(path)
  }

  def closeDirectory () : Unit = {
    val path = getPath( fileQueue.dequeue() )
   // mergeSummary(new Path(path) )
  }


}
