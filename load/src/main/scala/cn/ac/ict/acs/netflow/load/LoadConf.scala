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

object LoadConf {

  // ************************************************************************
  // for parquet file
  val META_FILE = "_metadata"
  val COMMON_META_FILE = "_common_metadata"
  val SUCCESS_FIME = "_SUCCESS"
  val TEMP_DIRECTORY = "_temporary"

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

  // ParquetwriterWrapper
  val LOAD_DIR_CREATION_INTERVAL = "netflow.load.dir.creation.interval"
  val CLOSE_DELAY = "netflow.close.delay"
  val REPORT_MASTER_DELAY = "netflow.report.master.delay"
  val REPORT_WORKER_DELAY = "netflow.report.worker.delay"

  // loadWroker
  val WRITER_NUMBER = "netflow.writer.number"
  val QUEUE_MAXPACKAGE_NUM = "netflow.queue.maxPackageNum" // >=1, default 100000000
  val QUEUE_WARN_THRESHOLD = "netflow.queue.WarnThreshold" // [0,100], default 70

  val UNSORTED_PACKETS_DIR = "netflow.load.unsorted.dir"

}

