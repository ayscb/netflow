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

import java.io.{ IOException, FileNotFoundException }

import _root_.parquet.hadoop.{ ParquetFileWriter, ParquetFileReader }

import akka.actor.ActorSelection

import org.apache.hadoop.fs.{ FileStatus, PathFilter, Path, FileSystem }

import cn.ac.ict.acs.netflow.load
import cn.ac.ict.acs.netflow.load.{ CombineStatus, LoadConf }
import cn.ac.ict.acs.netflow.load.LoadMessages.CombineFinished
import cn.ac.ict.acs.netflow.{ NetFlowException, NetFlowConf, Logging }

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

  private val dirPathStr = load.getPathByTime(timestamp, conf)
  setName(s"Combine server")

  override def run(): Unit = {
    logInfo(s"Combine server begins to combine $dirPathStr")
    try {
      val fs = FileSystem.get(conf.hadoopConfiguration)
      val fPath = new Path(dirPathStr)
      if (!validDirectory(fs, fPath)) {
        master ! CombineFinished(CombineStatus.UNKNOWN_DIRECTORY)
        return
      } // has send the message to master

      val maxRetryNum = 4
      var curTry = 0
      while (maxRetryNum != curTry) {
        combineFiles(fs, fPath) match {
          case ParquetState.WAIT =>
            Thread.sleep(60000 + curTry * 60000)
            curTry += 1
          case ParquetState.FINISH =>
            master ! CombineFinished(CombineStatus.FINISH)
            return
          case ParquetState.FAIL =>
            master ! CombineFinished(CombineStatus.IO_EXCEPTION)
            return
        }
      }

      if (maxRetryNum == curTry) {
        // delete the template's file.
        // Since we believe that the file in template is wrong file.
        mergeParquetFiles(fs, fPath)
        finishCombine(fs, fPath)
        master ! CombineFinished(CombineStatus.PARTIAL_FINISH)
        logInfo(s"delete template file and Combine partial files finished. ")
      }
    } catch {
      case e: IOException =>
        master ! CombineFinished(CombineStatus.IO_EXCEPTION)
        logError(s"Combine files IO exception. ${e.getMessage} ")
        logError(s"${e.getStackTrace}")
    }

  }

  /**
   * Valid this directory is a correct path
   * @param fs hadoop file system
   * @param dirPath directory path
   * @return
   */
  private def validDirectory(fs: FileSystem, dirPath: Path): Boolean = {
    val pathStr = dirPath.toUri.toString
    try {
      if (!fs.exists(dirPath)) {
        logError(s"The path $pathStr does not exist in HDFS ")
        master ! CombineFinished(CombineStatus.DIRECTORY_NOT_EXIST)
        return false
      }

      if (!fs.isDirectory(dirPath)) {
        logError(s"The path $pathStr should be directory in HDFS. ")
        master ! CombineFinished(CombineStatus.UNKNOWN_DIRECTORY)
        return false
      }

      val ls = fs.listStatus(dirPath)
      if (ls.isEmpty) {
        logError(s"There is nothing in directory $pathStr, so we can not combine this directory.")
        master ! CombineFinished(CombineStatus.UNKNOWN_DIRECTORY)
        return false
      }

      ls.exists(file => {
        val name = file.getPath.getName
        name.endsWith(".parquet") || name.equalsIgnoreCase(LoadConf.TEMP_DIRECTORY)
      }) match {
        case true => true
        case false =>
          logError(s"In directory ${dirPath.toUri.toString}, " +
            s"expect contain '.parquet' files or directory $pathStr, " +
            s"but now all of them don' t exist ")
          master ! CombineFinished(CombineStatus.UNKNOWN_DIRECTORY)
          false
      }

    } catch {
      case e: FileNotFoundException =>
        logError(s"The path $pathStr does not exist in HDFS ")
        master ! CombineFinished(CombineStatus.DIRECTORY_NOT_EXIST)
        false
      case e: IOException =>
        logError(s"Combine $pathStr directory error, for ${e.getMessage} ")
        logError(s"${e.getStackTrace}")
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
        if (filterFiles(0).getPath.getName.endsWith(LoadConf.TEMP_DIRECTORY)) {
          val tempFile: Array[FileStatus] = fs.listStatus(filterFiles(0).getPath)
          if (tempFile.nonEmpty) {
            logInfo(s"Current ${LoadConf.TEMP_DIRECTORY} contains ${tempFile.length} files ")
            logDebug(s"The files name are ${tempFile.map(_.getPath.getName).mkString(";")}")
            ParquetState.WAIT
          } else {
            mergeParquetFiles(fs, fPath)
          }

        } else {
          logError(s"expect ${LoadConf.TEMP_DIRECTORY} directory, " +
            s"but know is ${filterFiles(0).getPath.getName}")
          ParquetState.FAIL
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
      logInfo(s"Will try to combine the directory ${fPath.toUri.toString}")
      val outputStatus = fs.getFileStatus(fPath)
      val footers =
        ParquetFileReader.readAllFootersInParallel(conf.hadoopConfiguration, outputStatus)

      ParquetFileWriter.writeMetadataFile(conf.hadoopConfiguration, outputStatus.getPath, footers)

      // write _success File
      finishCombine(fs, fPath)
      ParquetState.FINISH

    } catch {
      case e: IOException =>
        log.warn(s"{ NetFlow : Could not write summary file for %s, error message %s. "
          .format(fPath, e.getMessage))
        fs.listStatus(fPath, new PathFilter {
          override def accept(path: Path): Boolean = {
            path.getName.startsWith("_")
          }
        }).foreach(file => fs.delete(file.getPath, true))
        ParquetState.FAIL
    }
  }

  /**
   * Delete the _temporary directory and write a success file
   * to stand for finish combine process
   * @param fs hadoop file system
   * @param fPath  directory path
   */
  private def finishCombine(fs: FileSystem, fPath: Path): Unit = {
    val pathStr = fPath.toUri.toString
    val tempDir = new Path(fPath, LoadConf.TEMP_DIRECTORY)

    // delete empty tempDir
    if (fs.exists(tempDir)) {
      val tmpFiles = fs.listStatus(tempDir)
      if(tmpFiles.nonEmpty) {
        logWarning(s"${tempDir.toUri.toString} already has ${tmpFiles.length}} files," +
          s" which are '${tmpFiles.map(_.getPath.toUri.toString).mkString(";")}'. " +
          s"All of them will be delete.")
      }
      fs.delete(tempDir, true)
      logInfo(s"Delete ${tempDir.toUri.toString}} ")
    }

    // write _success File
    fs.create(new Path(fPath, LoadConf.SUCCESS_FIME)).close()
    fs.close()
    logInfo(s"Successfully finish combine $pathStr ")
  }
}

