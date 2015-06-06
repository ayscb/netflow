package cn.ac.ict.acs.netflow.metrics

import java.io.{FileInputStream, InputStream}
import java.util.Properties

import scala.collection.mutable

import cn.ac.ict.acs.netflow.Logging

import scala.util.matching.Regex

class MetricsConfig(val configFile: Option[String]) extends Logging {

  private val DEFAULT_PREFIX = "*"
  private val INSTANCE_REGEX = "^(\\*|[a-zA-Z]+)\\.(.+)".r
  private val DEFAULT_METRICS_CONF_FILENAME = "metrics.properties"

  private[metrics] val properties = new Properties()
  private[metrics] var propertyCategories: mutable.HashMap[String, Properties] = null

  def initialize() {

    // If spark.metrics.conf is not set, try to get file in class path
    val isOpt: Option[InputStream] = configFile.map(new FileInputStream(_)).orElse {
      try {
        Option(getClass.getClassLoader.getResourceAsStream(DEFAULT_METRICS_CONF_FILENAME))
      } catch {
        case e: Exception =>
          logError("Error loading default configuration file", e)
          None
      }
    }

    isOpt.foreach { is =>
      try {
        properties.load(is)
      } finally {
        is.close()
      }
    }

    propertyCategories = subProperties(properties, INSTANCE_REGEX)
    if (propertyCategories.contains(DEFAULT_PREFIX)) {
      import scala.collection.JavaConversions._

      val defaultProperty = propertyCategories(DEFAULT_PREFIX)
      for { (inst, prop) <- propertyCategories
            if (inst != DEFAULT_PREFIX)
            (k, v) <- defaultProperty
            if (prop.getProperty(k) == null) } {
        prop.setProperty(k, v)
      }
    }
  }


  def subProperties(prop: Properties, regex: Regex): mutable.HashMap[String, Properties] = {
    val subProperties = new mutable.HashMap[String, Properties]
    import scala.collection.JavaConversions._
    prop.foreach { kv =>
      if (regex.findPrefixOf(kv._1).isDefined) {
        val regex(prefix, suffix) = kv._1
        subProperties.getOrElseUpdate(prefix, new Properties).setProperty(suffix, kv._2)
      }
    }
    subProperties
  }

  def getInstance(inst: String): Properties = {
    propertyCategories.get(inst) match {
      case Some(s) => s
      case None => propertyCategories.getOrElse(DEFAULT_PREFIX, new Properties)
    }
  }

}
