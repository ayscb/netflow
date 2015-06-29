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

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/**
 * Created by ayscb on 6/27/15.
 */
object hadoopTest {

  private def moveFile(outFile: Path, conf: Configuration): Unit ={
    val basePath = outFile.getParent.getParent
    val fileName = outFile.getName
    val fs = basePath.getFileSystem(conf)
    try {
      if (!fs.rename(outFile, new Path(basePath, fileName))) {
        printf(s"Can not remove file ${outFile.toUri.toString} " +
          s"to ${basePath.toUri.toString.concat("/").concat(fileName)}")
      }
    }catch {
      case e: IOException =>
        printf(s"Close parquet file error, ${e.getMessage} ")
        printf(s"${e.getStackTrace.toString}")
    }
  }

  def main(args: Array[String]) {
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://localhost:8020")
    val outputFile =
      new Path("/netflow/2015/03/16/10/50/_temporary/10.30.5.120-00-1435396714183.parquet")

    // move _template file to sub directory
    moveFile(outputFile, conf)
  }
}
