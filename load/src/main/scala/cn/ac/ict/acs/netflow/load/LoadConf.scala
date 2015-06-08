/**
 * Copyright 2015 ICT.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.ac.ict.acs.netflow.load

import parquet.hadoop.ParquetFileWriter

object LoadConf {

  // ************************************************************************
  // for parquet file
  val META_FILE = ParquetFileWriter.PARQUET_METADATA_FILE
  val COMMON_META_FILE = ParquetFileWriter.PARQUET_COMMON_METADATA_FILE
  val SUCCESS_FIME = "_SUCCESS"
  val TEMP_DICTIONARY = "_temporary"

  // ************************** config **************************************
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

  val dICTIONARY_INTERVAL = "netflow.parquet.dictionary.interval"
  val LOAD_INTERVAL = "netflow.load.interval"
  val LOAD_DATARATE = "netflow.load.dataRate"
  val LOAD_STARTTIME = "netflow.load.startTime"
  val LOAD_ENDTIME = "netflow.load.endTime"
  val LOAD_PATH = "netflow.load.path"

}
