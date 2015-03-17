package cn.ac.ict.acs.netflow

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

class NetFlowConf {

  private val settings = new ConcurrentHashMap[String, String]()

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
