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
package cn.ac.ict.acs.netflow

import java.util.Properties
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import cn.ac.ict.acs.netflow.util.Utils

object NetFlowConf {
  val DFS_NAME = "netflow.fs.default.name"
  val TIME_FORMAT = "netflow.time.format"

  val KB = 1024
  val MB = 1024 * KB
}

object LoadConf {
  val LOAD_INTERVAL = "netflow.load.interval"
  val LOAD_DATARATE = "netflow.load.dataRate"
  val LOAD_STARTTIME = "netflow.load.startTime"
  val LOAD_ENDTIME = "netflow.load.endTime"
  val LOAD_PATH = "netflow.load.path"
}


class NetFlowConf(loadDefaults: Boolean) extends Serializable {
  import LoadConf._
  import NetFlowConf._

  def this() = this(true)

  @transient private val settings = new ConcurrentHashMap[String, String]()

  if (loadDefaults) {
    // Load any netflow.* system properties that passed as -D<name>=<value> at start time
    for ((key, value) <- Utils.getSystemProperties if key.startsWith("netflow.")) {
      set(key, value)
    }
  }

    /** ************************ NetFLow Params/Hints ******************* */

  def dfsName = get(DFS_NAME, "hdfs://localhost:9000")

  def timeFormatStr = get(TIME_FORMAT, "yyyy-MM-dd:HH:mm")

  def timeFormat = DateTimeFormat.forPattern(timeFormatStr)



  /** ************************ Load Params/Hints ******************* */

  def loadInterval = getInt(LOAD_INTERVAL, 4)

  def loadRate = getLong(LOAD_DATARATE, 1L) * MB

  def loadStartInSec = DateTime.parse(get(LOAD_STARTTIME), timeFormat).getMillis / 1000

  def loadEndInSec = DateTime.parse(get(LOAD_ENDTIME), timeFormat).getMillis / 1000

  def loadPath = get(LOAD_PATH)




  /** ************************ Base Utils/Implementations ******************* */


  def load(path: String): NetFlowConf = {
    setAll(Utils.getPropertiesFromFile(path))
  }

  def set(props: Properties): NetFlowConf = {
    props.foreach { case (k, v) => settings.put(k, v) }
    this
  }

  /** Set a configuration variable. */
  def set(key: String, value: String): NetFlowConf = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value for " + key)
    }
    settings.put(key, value)
    this
  }

  /** Set multiple parameters together */
  def setAll(settings: Traversable[(String, String)]) = {
    this.settings.putAll(settings.toMap.asJava)
    this
  }

  /** Set a parameter if it isn't already configured */
  def setIfMissing(key: String, value: String): NetFlowConf = {
    settings.putIfAbsent(key, value)
    this
  }

  /** Remove a parameter from the configuration */
  def remove(key: String): NetFlowConf = {
    settings.remove(key)
    this
  }

  /** Get a parameter; throws a NoSuchElementException if it's not set */
  def get(key: String): String = {
    getOption(key).getOrElse(throw new NoSuchElementException(key))
  }

  /** Get a parameter, falling back to a default if not set */
  def get(key: String, defaultValue: String): String = {
    getOption(key).getOrElse(defaultValue)
  }

  /** Get a parameter as an Option */
  def getOption(key: String): Option[String] = {
    Option(settings.get(key))
  }

  /** Get all parameters as a list of pairs */
  def getAll: Array[(String, String)] = {
    settings.entrySet().asScala.map(x => (x.getKey, x.getValue)).toArray
  }

  /** Get a parameter as an integer, falling back to a default if not set */
  def getInt(key: String, defaultValue: Int): Int = {
    getOption(key).map(_.toInt).getOrElse(defaultValue)
  }

  /** Get a parameter as a long, falling back to a default if not set */
  def getLong(key: String, defaultValue: Long): Long = {
    getOption(key).map(_.toLong).getOrElse(defaultValue)
  }

  /** Get a parameter as a double, falling back to a default if not set */
  def getDouble(key: String, defaultValue: Double): Double = {
    getOption(key).map(_.toDouble).getOrElse(defaultValue)
  }

  /** Get a parameter as a boolean, falling back to a default if not set */
  def getBoolean(key: String, defaultValue: Boolean): Boolean = {
    getOption(key).map(_.toBoolean).getOrElse(defaultValue)
  }

  /** Does the configuration contain a given parameter? */
  def contains(key: String): Boolean = settings.containsKey(key)

}
