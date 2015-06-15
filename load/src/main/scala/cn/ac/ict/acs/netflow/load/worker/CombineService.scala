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
package cn.ac.ict.acs.netflow.load.worker

import java.io.{IOException, FileNotFoundException}

import _root_.parquet.hadoop.{ParquetFileWriter, ParquetFileReader}
import org.apache.hadoop.fs.{PathFilter, Path, FileSystem}

import akka.actor.{ActorSelection, ActorRef}

import cn.ac.ict.acs.netflow.load.{CombineStatus, LoadConf}
import cn.ac.ict.acs.netflow.load.LoadMessages.CombineFinished
import cn.ac.ict.acs.netflow.{NetFlowException, NetFlowConf, Logging}
import cn.ac.ict.acs.netflow.util.TimeUtils

/**
 * Combine the parquet directory
 * Created by ayscb on 15-6-15.
 */
class CombineService(val timestamp: Long, val master: ActorSelection, val conf: NetFlowConf)
  extends Thread with Logging {


  object ParquetState extends Enumeration {
    type ParquetState = Value
    val FINISH, FAIL, WAIT = Value
  }

  val dirPathStr = TimeUtils getTimeBasePathBySeconds(timestamp, conf)
  setName(s"Combine server on directory $dirPathStr ")

  override def run(): Unit = {
    logInfo(s"Combine server begins to combine $dirPathStr ...")
    val fs = FileSystem.get(conf.hadoopConfiguration)
    val fPath = new Path(dirPathStr)
    if (!validDirectory(fs, fPath)) return

    val maxRetryNum = 4
    var curTry = 0
    while(maxRetryNum == curTry) {
      combineFiles(fs, fPath) match {
        case ParquetState.WAIT =>
          Thread.sleep(3000 + curTry * 30000)
          curTry += 1
        case ParquetState.FINISH =>
          master ! CombineFinished(CombineStatus.FINISH)
          return
        case ParquetState.FAIL =>
          master ! CombineFinished(CombineStatus.IO_EXCEPTION)
          return
      }
    }

    if(maxRetryNum == curTry){
      master ! CombineFinished(CombineStatus.WAIT_TIMEOUT)
    }
  }

  /**
   * Valid this directory is a correct path
   * @param fs hadoop file system
   * @param dirPath directory path
   * @return
   */
  private def validDirectory(fs: FileSystem, dirPath: Path): Boolean = {

    try {
      if (!fs.exists(dirPath)) {
        logError("[ parquet ] The path %s does not exist in Hadoop ".format(dirPath.toUri.toString))
        master ! CombineFinished(CombineStatus.DIRECTORY_NOT_EXIST)
        return false
      }

      if (!fs.isDirectory(dirPath)) {
        logError("[ parquet ] The path %s should be directory in Hadoop ".format(dirPath.toUri.toString))
        master ! CombineFinished(CombineStatus.UNKNOWN_DIRECTORY)
        return false
      }

      val ls = fs.listStatus(dirPath)
      if (ls.isEmpty) {
        master ! CombineFinished(CombineStatus.UNKNOWN_DIRECTORY)
        return false
      }

      ls.exists(file => {
        val name = file.getPath.getName
        name.endsWith(".parquet") || name.equalsIgnoreCase(LoadConf.TEMP_DIRECTORY)
      }) match {
        case true => true
        case false =>
          master ! CombineFinished(CombineStatus.UNKNOWN_DIRECTORY)
          false
      }

    } catch {
      case e: FileNotFoundException =>
        logWarning("[ parquet ] The path %s does not exist in Hadoop ".format(dirPath.toUri.toString))
        master ! CombineFinished(CombineStatus.DIRECTORY_NOT_EXIST)
        false
      case e: IOException =>
        logWarning("[ parquet ]IOException in %s. ".format(dirPath.toUri.toString))
        master ! CombineFinished(CombineStatus.IO_EXCEPTION)
        false
    }
  }

  /**
   * only all sub parquet files are ready that we can combine the parquet file
   * @param fs hadoop file system
   * @param fPath  directory path
   * @return
   */
  private def combineFiles(fs: FileSystem, fPath: Path): ParquetState.Value = {

    val filterFiles =
      fs.listStatus(fPath).filter(filter => filter.getPath.getName.startsWith("_"))

    /*
      we may face with these situation:
        1:  _temp

        2:  _temp + _common ( fail )
            _temp + _meta   ( fail )
            _meta + _common ( success )

        3:  _temp + _common + _meta     ( fail )
            _common + _meta + _Success  ( success )
     */
    filterFiles.length match {
      case 0 =>
        throw new NetFlowException("Filter file should no be 0")

      case 1 => // only _temporary directory
        logInfo(("[ parquet ] we will combine all parquet' files only " +
          "when %s is empty. ").format(filterFiles(0).getPath.getName))
        val tempFile = fs.listStatus(filterFiles(0).getPath)
        if (tempFile.nonEmpty) {
          ParquetState.WAIT
        } else {
          mergeParquetFiles(fs, fPath)
        }

      case 2 | 3 =>
        // if the LoadConf.TEMP_DIRECTORY is exist, delete current _meta and _common_meta
        filterFiles.find(x => x.getPath.getName.equals(LoadConf.TEMP_DIRECTORY)) match {
          case Some(tmpfile) =>
            require(fs.listStatus(tmpfile.getPath).isEmpty)
            filterFiles
              .filterNot(x => x.getPath.getName.equals(LoadConf.TEMP_DIRECTORY))
              .foreach(file => fs.delete(file.getPath, true))
            mergeParquetFiles(fs, fPath)
          case None =>
            if (filterFiles.length == 2) {
              finishCombine(fs, fPath)
            }
            ParquetState.FINISH
        }

      case _ =>
        logError("[ parquet ] TOO many '_xxx' files in path %s ".format(
          filterFiles.head.getPath.getParent.toString))
        ParquetState.FAIL
    }
  }


  /**
   * merge all parquet files in to output path
   * @param fs hadoop file system
   * @param fPath  directory path
   */
  private def mergeParquetFiles(fs: FileSystem, fPath: Path): ParquetState.Value = {

    try {
      val outputStatus = fs.getFileStatus(fPath)
      val footers = ParquetFileReader.readAllFootersInParallel(conf.hadoopConfiguration, outputStatus)
      ParquetFileWriter.writeMetadataFile(conf.hadoopConfiguration, fPath, footers)

      // write _success File
      finishCombine(fs, fPath)
      ParquetState.FINISH

    } catch {
      case e: IOException =>
        log.warn(s"{ NetFlow : Could not write summary file for %s, error message %s. ".format(fPath, e.getMessage))
        fs.listStatus(fPath, new PathFilter {
          override def accept(path: Path): Boolean = {
            path.getName.startsWith("_")
          }
        }).foreach(file => fs.delete(file.getPath, true))
        ParquetState.FAIL
    }
  }

  /**
   * Delete the _temporary directory and write a success file to stand for finish combine process
   * @param fs hadoop file system
   * @param fPath  directory path
   */
  private def finishCombine(fs: FileSystem, fPath: Path): Unit = {
    val tempDir = new Path(fPath, LoadConf.TEMP_DIRECTORY)

    // delete empty tempDir
    if (fs.exists(tempDir)) {
      require(fs.listStatus(tempDir).isEmpty, "The %s /_temporary should be empty, but now it has %d elements. "
        .format(fPath.toUri.toString, fs.listStatus(tempDir).size))

      fs.delete(tempDir, true)
      logInfo("[ Parquet ] Combine %s's parquets finished, Delete the %s ".
        format(tempDir.toUri.toString, LoadConf.TEMP_DIRECTORY))
    }

    // write _success File
    fs.create(new Path(fPath, LoadConf.SUCCESS_FIME)).close()
    fs.close()
  }
}



