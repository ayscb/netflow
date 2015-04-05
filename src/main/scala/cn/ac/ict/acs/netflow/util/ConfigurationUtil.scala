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
package cn.ac.ict.acs.netflow.util

import java.nio.file.{Paths, Files}
import java.util.Properties

import scala.io.Source

object ConfigurationUtil {

  /**
   * Loads the configuration file for the application, if it exists. This is either the
   * user-specified properties file, or the spark-defaults.conf file under the Spark configuration
   * directory.
   */
  def loadPropertiesFile(path: String): Properties = {
    val props = new Properties

    checkArgument(Files.isRegularFile(Paths.get(path)), "Invalid properties file " + path)

    val src = Source.fromFile(path)
    props.load(src.bufferedReader())
    src.close()

    props
  }

  def checkArgument(check: Boolean, msg: String, args: Any*) {
    if (!check) {
      throw new IllegalArgumentException(String.format(msg, args))
    }
  }

}
