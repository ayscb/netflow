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
package cn.ac.ict.acs.netflow.load.util
import org.apache.hadoop.conf.Configuration

import org.apache.hadoop.fs.{ FileStatus, FileSystem, Path }
import parquet.hadoop.{ ParquetFileReader, ParquetFileWriter }

import cn.ac.ict.acs.netflow.{ Logging, NetFlowConf }

/**
 * merge all parquets's meta infomation in a assigned document to a single file
 * Created by ayscb on 2015/4/23.
 */
object ParquetState extends Enumeration {
  type ParquetState = Value
  val DIC_NOT_EXIST, NO_DIC, DIC_EMPTY, NO_PARQUET, UNREADY, FINISH, FAIL, STRUCT_ERROR = Value
}

object NetFlowCombineMeta extends Logging {


  private val META_FILE = ParquetFileWriter.PARQUET_METADATA_FILE
  private val SUCCESS_FIME = "_SUCCESS"
  private val TEMP_DICT = "_temporary"

  private var combineStatus : ParquetState.Value = _
  /**
   *  merge all parquet files in to outputpath
   *
   * @param outputPath  least path
   * @param conf
   */
  private def mergeParquetFiles(outputPath: Path, conf: NetFlowConf): ParquetState.Value = {
    val hadoopConf = new Configuration()

    try {
      log.info(s"[ NetFlow : ")
      val fileSystem = outputPath.getFileSystem(hadoopConf)
      val outputStatus = fileSystem.getFileStatus(outputPath)
      val footers = ParquetFileReader.readAllFootersInParallel(hadoopConf, outputStatus)

      try {
        ParquetFileWriter.writeMetadataFile(hadoopConf, outputPath, footers)

        // delete the _temporary dic
        val tempFileNum = fileSystem.listStatus(new Path(outputPath, TEMP_DICT)).length
        require(tempFileNum == 0,
          "The %s /_temporary should be empty, but now it has %d elements. "
            .format(outputPath.toUri.toString, tempFileNum))

        logInfo("[ Parquet ] Combine %s's parquets finished, Delete the %s ".
          format(outputPath.toUri.toString, TEMP_DICT))
        fileSystem.delete(new Path(outputPath, TEMP_DICT), true)

        // write _success File
        fileSystem.create(new Path(outputPath, SUCCESS_FIME)).close()

        fileSystem.close()
        logInfo("[ Parquet ] Write _sunncess file ")
        ParquetState.FINISH
      } catch {
        case e: Exception =>
          log.warn(s"{ NetFlow : Could not write summary file for " +
            outputPath + " , Message: " + e.getMessage + " ]")
          val metadataPath = new Path(outputPath, META_FILE)
          if (fileSystem.exists(outputPath)) {
            fileSystem.delete(metadataPath, true)
          }
          ParquetState.FAIL
      } finally {
        fileSystem.close()
      }
    }
  }

  private def finishCombine(fs: FileSystem, fPath : Path): ParquetState.Value ={
    val tempDir = new Path(fPath, TEMP_DICT)

    // delete empty tempDir
    if(fs.exists(tempDir)) {
      require(fs.listStatus(tempDir).isEmpty)
      fs.delete(tempDir,true)
      logInfo("[ Parquet ] Combine %s's parquets finished, Delete the %s ".
        format(tempDir.toUri.toString, TEMP_DICT))
    }

    // write _success File
    fs.create(new Path(fPath, SUCCESS_FIME)).close()

    ParquetState.FINISH
  }

  /**
   * only all sub parquet files are ready that
   * we can combine the parquet file
   * @param fs
   * @param path
   * @return
   */
  def combineFiles(fs: FileSystem, path: Path, conf: NetFlowConf): ParquetState.Value = {
    if (!fs.exists(path)) {
      logError("[ parquet ] The path %s does not exist in Hadoop ".format(path.toUri.toString))
      return ParquetState.DIC_NOT_EXIST
    }

    if (!fs.isDirectory(path)) {
      logError("[ parquet ] The path %s should be dictionary in Hadoop ".
        format(path.toUri.toString))
      return ParquetState.NO_DIC
    }

    val fsList: Array[FileStatus] = fs.listStatus(path)
    if (fsList.length == 0) return ParquetState.DIC_EMPTY

    val filterFiles: Array[FileStatus] = fsList.filter(
      filter => filter.getPath.getName.startsWith("_"))

    // situation:
    // 1: _temp

    // 2: _temp + _common ( fail )
    //    _temp + _meta ( fail )
    //    _meta + _common ( success )

    // 3: _temp + _common + _meta  ( fail )
    //    _common + _meta + _Success ( success )

    filterFiles.length match {
      case 0 =>
        logError("[ parquet ] The path %s is not a parquet dictionary. ".format(path.getName))
        ParquetState.NO_PARQUET

      case 1 => // only _temp [D]
        logInfo(("[ parquet ] we will combine all parquet' files only " +
          "when there is empty in path %s ").format(filterFiles(0).getPath.getName))
        val tempFile = fs.listStatus(filterFiles(0).getPath)
        if (tempFile.nonEmpty) {
          ParquetState.UNREADY
        } else {
          mergeParquetFiles(path, conf)
        }

      case 2 | 3 =>
        filterFiles.find(x => x.getPath.getName.equals(TEMP_DICT)) match {
          case Some(tmpfile) =>
            require(fs.listStatus(tmpfile.getPath).isEmpty)
            filterFiles.filterNot(x=>x.getPath.getName.equals(TEMP_DICT)).foreach(file=>{
              fs.delete(file.getPath,true)
            })
            mergeParquetFiles(path, conf)
          case None =>
            if(filterFiles.length == 2){
              // will add file 'SUCCESS'
              fs.create(new Path(path, SUCCESS_FIME)).close()
            }
        }
        ParquetState.FINISH

      case _ =>
        logError("[ parquet ] TOO many '_xxx' files in path %s ".format(
          filterFiles.head.getPath.getParent.toString))
        ParquetState.STRUCT_ERROR
    }
  }
}
