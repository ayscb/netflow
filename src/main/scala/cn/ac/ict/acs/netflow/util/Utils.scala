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

import java.io.{IOException, FileInputStream, InputStreamReader, File}
import java.net.{Inet4Address, NetworkInterface, InetAddress, BindException}
import java.util.Properties

import org.eclipse.jetty.util.MultiException

import scala.collection.JavaConversions._

import cn.ac.ict.acs.netflow.{Logging, NetFlowConf, NetFlowException}

import scala.collection.Map

object Utils extends Logging {

  /**
   * Attempt to start a service on the given port, or fail after a number of attempts.
   * Each subsequent attempt uses 1 + the port used in the previous attempt (unless the port is 0).
   *
   * @param startPort The initial port to start the service on.
   * @param startService Function to start service on a given port.
   *                     This is expected to throw java.net.BindException on port collision.
   * @param conf A NetFlowConf used to get the maximum number of retries when binding to a port.
   * @param serviceName Name of the service.
   */
  def startServiceOnPort[T](
      startPort: Int,
      startService: Int => (T, Int),
      conf: NetFlowConf,
      serviceName: String = ""): (T, Int) = {

    require(startPort == 0 || (1024 <= startPort && startPort < 65536),
      "startPort should be between 1024 and 65535 (inclusive), or 0 for a random free port.")

    val serviceString = if (serviceName.isEmpty) "" else s" '$serviceName'"
    val maxRetries = portMaxRetries(conf)
    for (offset <- 0 to maxRetries) {
      // Do not increment port if startPort is 0, which is treated as a special port
      val tryPort = if (startPort == 0) {
        startPort
      } else {
        // If the new port wraps around, do not try a privilege port
        ((startPort + offset - 1024) % (65536 - 1024)) + 1024
      }
      try {
        val (service, port) = startService(tryPort)
        logInfo(s"Successfully started service$serviceString on port $port.")
        return (service, port)
      } catch {
        case e: Exception if isBindCollision(e) =>
          if (offset >= maxRetries) {
            val exceptionMessage =
              s"${e.getMessage}: Service$serviceString failed after $maxRetries retries!"
            val exception = new BindException(exceptionMessage)
            // restore original stack trace
            exception.setStackTrace(e.getStackTrace)
            throw exception
          }
          logWarning(s"Service$serviceString could not bind on port $tryPort. " +
            s"Attempting port ${tryPort + 1}.")
      }
    }
    // Should never happen
    throw new NetFlowException(s"Failed to start service$serviceString on port $startPort")
  }

  /**
   * Maximum number of retries when binding to a port before giving up.
   */
  def portMaxRetries(conf: NetFlowConf): Int = {
    conf.getInt("netflow.port.maxRetries", 16)
  }

  /**
   * Return whether the exception is caused by an address-port collision when binding.
   */
  def isBindCollision(exception: Throwable): Boolean = {
    exception match {
      case e: BindException =>
        if (e.getMessage != null) {
          return true
        }
        isBindCollision(e.getCause)
      case e: MultiException => e.getThrowables.exists(isBindCollision)
      case e: Exception => isBindCollision(e.getCause)
      case _ => false
    }
  }

  def checkHost(host: String, message: String = "") {
    assert(host.indexOf(':') == -1, message)
  }

  def checkHostPort(hostPort: String, message: String = "") {
    assert(hostPort.indexOf(':') != -1, message)
  }

  def getAddressHostName(address: String): String = {
    InetAddress.getByName(address).getHostName
  }

  private var customHostname: Option[String] = sys.env.get("NETFLOW_LOCAL_HOSTNAME")

  /**
   * Get the local host's IP address in dotted-quad format (e.g. 1.2.3.4).
   * Note, this is typically not used from within core spark.
   */
  lazy val localIpAddress: String = findLocalIpAddress()
  lazy val localIpAddressHostname: String = getAddressHostName(localIpAddress)

  /**
   * Get the local machine's hostname.
   */
  def localHostName(): String = {
    customHostname.getOrElse(localIpAddressHostname)
  }

  def findLocalIpAddress(): String = {
    val defaultIpOverride = System.getenv("NETFLOW_LOCAL_IP")
    if (defaultIpOverride != null) {
      defaultIpOverride
    } else {
      val address = InetAddress.getLocalHost
      if (address.isLoopbackAddress) {
        // Address resolves to something like 127.0.1.1, which happens on Debian; try to find
        // a better address using the local network interfaces
        // getNetworkInterfaces returns ifs in reverse order compared to ifconfig output order
        // on unix-like system. On windows, it returns in index order.
        // It's more proper to pick ip address following system output order.
        val activeNetworkIFs = NetworkInterface.getNetworkInterfaces.toList
        val reOrderedNetworkIFs = activeNetworkIFs.reverse
        for (ni <- reOrderedNetworkIFs) {
          for (addr <- ni.getInetAddresses if !addr.isLinkLocalAddress &&
            !addr.isLoopbackAddress && addr.isInstanceOf[Inet4Address]) {
            // We've found an address that looks reasonable!
            logWarning("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to" +
              " a loopback address: " + address.getHostAddress + "; using " + addr.getHostAddress +
              " instead (on interface " + ni.getName + ")")
            logWarning("Set SPARK_LOCAL_IP if you need to bind to another address")
            return addr.getHostAddress
          }
        }
        logWarning("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to" +
          " a loopback address: " + address.getHostAddress + ", but we couldn't find any" +
          " external IP address!")
        logWarning("Set SPARK_LOCAL_IP if you need to bind to another address")
      }
      address.getHostAddress
    }
  }

  def main (args: Array[String]) {
    println(findLocalIpAddress())
  }

  /**
   * Load default NetFlow properties from the given file. If no file is provided,
   * use the common defaults file. This mutates state in the given NetFlowConf and
   * in this JVM's system properties if the config specified in the file is not
   * already set. Return the path of the properties file used.
   */
  def loadDefaultProperties(conf: NetFlowConf, filePath: String = null): String = {
    val path = Option(filePath).getOrElse(getDefaultPropertiesFile())
    Option(path).foreach { confFile =>
      getPropertiesFromFile(confFile).filter { case (k, v) =>
        k.startsWith("netflow.")
      }.foreach { case (k, v) =>
        conf.setIfMissing(k, v)
        sys.props.getOrElseUpdate(k, v)
      }
    }
    path
  }

  /** Load properties present in the given file. */
  def getPropertiesFromFile(filename: String): Map[String, String] = {
    val file = new File(filename)
    require(file.exists(), s"Properties file $file does not exist")
    require(file.isFile(), s"Properties file $file is not a normal file")

    val inReader = new InputStreamReader(new FileInputStream(file), "UTF-8")
    try {
      val properties = new Properties()
      properties.load(inReader)
      properties.stringPropertyNames().map(k => (k, properties(k).trim)).toMap
    } catch {
      case e: IOException =>
        throw new NetFlowException(s"Failed when loading NetFlow properties from $filename", e)
    } finally {
      inReader.close()
    }
  }

  /** Return the path of the default Spark properties file. */
  def getDefaultPropertiesFile(env: Map[String, String] = sys.env): String = {
    env.get("NETFLOW_CONF_DIR")
      .orElse(env.get("NETFLOW_HOME").map { t => s"$t${File.separator}conf" })
      .map { t => new File(s"$t${File.separator}netflow-defaults.conf")}
      .filter(_.isFile)
      .map(_.getAbsolutePath)
      .orNull
  }

}
