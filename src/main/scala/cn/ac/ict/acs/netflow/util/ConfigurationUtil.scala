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
