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
package cn.as.ict.acs.netflow.load.worker

import java.io.{IOException, FileNotFoundException}

import cn.ac.ict.acs.netflow.load.LoadConf

import cn.ac.ict.acs.netflow.{NetFlowException, load, Logging, NetFlowConf}
import org.apache.hadoop.fs.{PathFilter, Path, FileSystem}
import parquet.hadoop.{ParquetFileWriter, ParquetFileReader}

object CombineServiceT{

  val conf = new NetFlowConf()
  conf.hadoopConfiguration.set("fs.defaultFS","hdfs://localhost:8020")
  val baseT = load.getTimeBase(1426474260, conf) * 1000

  println(baseT)
  def main(args: Array[String]) {

    val com = new CombineServiceT(baseT,conf)
    com.start()
    Thread.currentThread().join()
  }
}


class CombineServiceT(val timestamp: Long, val conf: NetFlowConf)
  extends Thread with Logging {

  object ParquetState extends Enumeration {
    type ParquetState = Value
    val FINISH, FAIL, WAIT = Value
  }

  private val dirPathStr = "/netflow/2015/03/16/10/50"
  setName(s"Combine server")

  override def run(): Unit = {
    logInfo(s"Combine server begins to combine $dirPathStr")
    val fs = FileSystem.get(conf.hadoopConfiguration)
    val fPath = new Path(dirPathStr)
    if (!validDirectory(fs, fPath)) {
      logInfo(s"Combine server error, validDirectory error!")
      return
    } // has send the message to master

    val maxRetryNum = 4
    var curTry = 0
    while (maxRetryNum != curTry) {
      combineFiles(fs, fPath) match {
        case ParquetState.WAIT =>
          Thread.sleep(60000 + curTry * 30000)
          curTry += 1
        case ParquetState.FINISH =>
          logInfo(s"Combine files finished. ")
          return
        case ParquetState.FAIL =>
          logInfo(s"Combine files IO exception. ")
          return
      }
    }

    if (maxRetryNum == curTry) {
      // delete the template's file.
      // Since we believe there
      mergeParquetFiles(fs, fPath)
      finishCombine(fs, fPath)
      logInfo(s"delete template file and Combine partial files finished. ")
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
        logError("[ parquet ] The path %s does not exist in Hadoop "
          .format(dirPath.toUri.toString))
        return false
      }

      if (!fs.isDirectory(dirPath)) {
        logError("[ parquet ] The path %s should be directory in Hadoop "
          .format(dirPath.toUri.toString))
        return false
      }

      val ls = fs.listStatus(dirPath)
      if (ls.isEmpty) {
        logError("[ parquet ] UNKNOWN_DIRECTORY ")
        return false
      }

      ls.exists(file => {
        val name = file.getPath.getName
        name.endsWith(".parquet") || name.equalsIgnoreCase(LoadConf.TEMP_DIRECTORY)
      }) match {
        case true => true
        case false =>
          logError("[ parquet ] UNKNOWN_DIRECTORY ")
          false
      }

    } catch {
      case e: FileNotFoundException =>
        logWarning("[ parquet ] The path %s does not exist in Hadoop "
          .format(dirPath.toUri.toString))
        false
      case e: IOException =>
        logWarning("[ parquet ]IOException in %s. ".format(dirPath.toUri.toString))
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
     //   mergeParquetFiles(fs, fPath)
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
      val footers =
        ParquetFileReader.readAllFootersInParallel(conf.hadoopConfiguration, outputStatus)

      ParquetFileWriter.writeMetadataFile(conf.hadoopConfiguration, fs.getFileStatus(fPath).getPath, footers)

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
    val tempDir = new Path(fPath, LoadConf.TEMP_DIRECTORY)

    // delete empty tempDir
    if (fs.exists(tempDir)) {
      logInfo(s"The ${fPath.toUri.toString}/_temporary's file size" +
        s" ${fs.listStatus(tempDir).size}}.")

      fs.delete(tempDir, true)
      logInfo("[ Parquet ] Combine %s's parquets finished, Delete the %s ".
        format(tempDir.toUri.toString, LoadConf.TEMP_DIRECTORY))
    }

    // write _success File
    fs.create(new Path(fPath, LoadConf.SUCCESS_FIME)).close()
    fs.close()
  }
}
