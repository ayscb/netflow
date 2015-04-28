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

<<<<<<< HEAD
import org.apache.hadoop.conf.{Configuration, Configured}
=======
import cn.ac.ict.acs.netflow.util.Utils
>>>>>>> sql-1/master
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormatter, DateTimeFormat}

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

object NetFlowConf {
  val DFS_NAME = "netflow.fs.default.name"
  val DOC_TIME_INTERVAL = "netflow.document.time.interval"      // the document we will organize ,s( seconds )
  val NETFLOW_BASE_ROOT = " netflow.base.root"

  val TIME_FORMAT = "netflow.time.format"     // for test

  val TIME_PATH_FORMAT="netflow.time.path.format"
  val KB = 1024
  val MB = 1024 * KB
}

// for test
object LoadConf {
  val LOAD_INTERVAL =   "netflow.load.interval"
  val LOAD_DATARATE =   "netflow.load.dataRate"
  val LOAD_STARTTIME =  "netflow.load.startTime"
  val LOAD_ENDTIME =    "netflow.load.endTime"
  val LOAD_PATH =       "netflow.load.path"
}

<<<<<<< HEAD
class NetFlowConf extends Serializable {
=======

class NetFlowConf(loadDefaults: Boolean) extends Serializable {
>>>>>>> sql-1/master
  import NetFlowConf._
  import LoadConf._

  def this() = this(true)

  @transient private val settings = new ConcurrentHashMap[String, String]()

<<<<<<< HEAD
  /** ************************ NetFLow Params/Hints ******************* */
=======
  if (loadDefaults) {
    // Load any netflow.* system properties that passed as -D<name>=<value> at start time
    for ((key, value) <- Utils.getSystemProperties if key.startsWith("netflow.")) {
      set(key, value)
    }
  }

    /** ************************ NetFLow Params/Hints ******************* */
>>>>>>> sql-1/master

  def dfsName = get(DFS_NAME, "hdfs://localhost:9000")

  def timeFormat: DateTimeFormatter = {
    val timeFormatStr = get(TIME_FORMAT, "yyyy-MM-dd:HH:mm")
    DateTimeFormat.forPattern(timeFormatStr)
  }

  def doctTimeIntervalValue :String = get(DOC_TIME_INTERVAL,"600")

  def doctTimeIntervalFormat:DateTimeFormatter = {
    // 10 min as default
    val value = get(DOC_TIME_INTERVAL, "600").toLong
    val strFormat : String = value match {
      case x  if x < 60    => "/yyyy/MM/dd/HH/mm/ss" // second level
      case x if x < 3600  =>  "/yyyy/MM/dd/HH/mm" // minute level
      case x if x < 86400 =>  "/yyyy/MM/dd/" // hour level
      case _              =>  "/yyyy/MM/dd/HH/"
    }
    DateTimeFormat.forPattern(strFormat.toString)
  }

  def getBaseRoot = get(NETFLOW_BASE_ROOT,"netflow")

  lazy val conf = {
    val _conf = new Configuration()
    val s = _conf.get("fs.defaultFS")
    if( _conf.get("fs.defaultFS").startsWith("file")){
      _conf.set("fs.defaultFS",dfsName)
    }
    _conf
  }

  def hadoopConfigure = conf
  /** ************************ Load Params/Hints ******************* */

  def loadInterval = getInt(LOAD_INTERVAL, 4)

  def loadRate = getLong(LOAD_DATARATE, 1L) * MB

  def loadStartInSec = DateTime.parse(get(LOAD_STARTTIME), timeFormat).getMillis / 1000

  def loadEndInSec = DateTime.parse(get(LOAD_ENDTIME), timeFormat).getMillis / 1000

  def loadPath = getAbsolutePath(get(LOAD_PATH))    // we get the path which should begin with "\"


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
  def getFloat(key :String,  defaultValue: Float) : Float = {
    getOption(key).map(_.toFloat).getOrElse(defaultValue)
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


  /** ******************************* some tools ***************************** **/
  private def getAbsolutePath( path :String): String =
    if ( path.startsWith("/") ) path else "/".concat(path)

  //get the format level
  private def getTimeStrFromInterval( timeStr : String) = {
      timeStr.toLong match {
      case x => if ( x< 60 )      "/yyyy/MM/dd/HH/mm/"        // second level
      case x => if ( x < 3600 )   "/yyyy/MM/dd/HH/"           // minute level
      case x => if ( x < 86400 )  "/yyyy/MM/dd/"              // hour level
    }
  }
}
