package cn.ac.ict.acs.netflow.load2

import parquet.hadoop.ParquetFileWriter

/**
 * Created by ayscb on 2015/4/29.
 */
object NetFlowArgument {

  //************************************************************************
  // for parquet file
  val META_FILE = ParquetFileWriter.PARQUET_METADATA_FILE
  val COMMON_META_FILE = ParquetFileWriter.PARQUET_COMMON_METADATA_FILE
  val SUCCESS_FIME = "_SUCCESS"
  val TEMP_DICTIONARY = "_temporary"

  //************************** config **************************************
  // NetflowWriterUtil
  val BLOCK_SIZE: String = "netflow.parquet.block.size"
  val PAGE_SIZE: String = "netflow.parquet.page.size"
  val COMPRESSION: String = "netflow.parquet.compression"
  val ENABLE_DICTIONARY: String = "netflow.parquet.enable.dictionary"
  val DICTIONARY_PAGE_SIZE: String = "netflow.parquet.dictionary.page.size"
  val VALIDATION: String = "netflow.parquet.validation"
  val WRITER_VERSION: String = "netflow.parquet.writer.version"

  val ENABLE_MEMORYMANAGER = "netflow.parquet.memory.manage.enable"
  val MEMORY_POOL_RATIO: String = "netflow.parquet.memory.pool.ratio"
  val MIN_MEMORY_ALLOCATION: String = "netflow.parquet.memory.min.chunk.size"

  val ZOOKEEPER_URL = "netflow.deploy.zookeeper.url"
  val ZOOKEEPER_DIR = "netflow.deploy.zookeeper.dir"
  val RECOVERY_DIR = "netflow.deploy.recoveryDirectory"

}
