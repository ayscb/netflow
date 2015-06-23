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
package cn.ac.ict.acs.netflow.load.master

import java.io.IOException
import java.net.{ServerSocket, InetSocketAddress}
import java.nio.{ByteOrder, ByteBuffer}
import java.nio.channels._

import akka.actor.ActorRef
import cn.ac.ict.acs.netflow.load.LoadMessages.{DeleReceiver, RequestWorker, DeleWorker}
import cn.ac.ict.acs.netflow.{NetFlowException, Logging, NetFlowConf}
import cn.ac.ict.acs.netflow.util.Utils

import scala.collection.mutable

class MasterService(val master: ActorRef, val conf: NetFlowConf)
  extends Thread  with Logging {

  // get the tcp thread runner
  private var actualPort: Int = 0
  private var selector: Selector = _

  // contain all socket include serverSocketChannel and all SocketChannel
  private val channels = mutable.HashSet.empty[Channel]

  // contain its ip str and channel
  private val ipToChannel = mutable.HashMap.empty[String,SocketChannel]

  def getActualPort: Int= {
    val retryTimes = 4
    var i = 0
    while(i != retryTimes){
      if(actualPort != 0) return actualPort
      Thread.sleep(i * 500)
      i += 1
    }
    actualPort
  }

  def collector2Socket = ipToChannel

  override def run(): Unit = {
    val serverSocketChannel = ServerSocketChannel.open()
    serverSocketChannel.configureBlocking(false)
    val serverSocket = serverSocketChannel.socket()
    channels += serverSocketChannel

    val port = conf.getInt("netflow.receiver.port", 10012)

    val service = Utils.startServiceOnPort(port, startListening(serverSocket), conf, "Collector Server")
    actualPort = service._2

    logInfo(s"Collector Server is listening $actualPort for connecting collector")

    selector = Selector.open()
    serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT)

    try {
      while (!this.isInterrupted) {
        try {
          // Blocking until channel of interest appears
          selector.select()

          val iter = selector.selectedKeys().iterator()
          while (iter.hasNext) {
            val key = iter.next()
            if (key.isAcceptable) {
              registerChannel(key)
            } else if (key.isReadable) {
              readPacketFromSocket(key)
            }
            iter.remove()
          }
        } catch {
          case e: InterruptedException =>
            logInfo(s"Collector Server is stopped. ")
            throw new InterruptedException
          case e: IOException =>
          // TODO
        }
      }
    } finally {
      // When a selector is closed, all channels registered with that selector are deregistered,
      // and the associated keys are invalidated(cancelled)
      logWarning("collector server terminated")
      selector.close()
      channels.foreach(_.close())
    }
  }

  private def startListening(serverSocket: ServerSocket): (Int) => (Any, Int) = { tryPort =>
    if (tryPort == 0) {
      serverSocket.bind(null)
    } else {
      serverSocket.bind(new InetSocketAddress(tryPort))
    }
    (null, serverSocket.getLocalPort)
  }

  private def getRemoteIp(socketChannel: SocketChannel): String ={
    socketChannel.getRemoteAddress.asInstanceOf[InetSocketAddress]
      .getAddress.getHostAddress
  }

  /** Accept remote connection */
  private def registerChannel(sk: SelectionKey): Unit = {
    // connect socket
    val socketChannel: SocketChannel = sk.channel().asInstanceOf[ServerSocketChannel].accept()
    socketChannel.configureBlocking(false)
    socketChannel.register(selector, SelectionKey.OP_READ).attach(new PacketHolder)

    val _remoteIP = getRemoteIp(socketChannel)
    channels += socketChannel
    ipToChannel(_remoteIP) = socketChannel
    logInfo(s"Open connection from ${_remoteIP}.")
  }

  /** Read at most 1 packet from the connection */
  private def readPacketFromSocket(sk: SelectionKey): Unit = {
    def channelRead(channel: SocketChannel, buffer: ByteBuffer): Unit = {
      if (channel.read(buffer) < 0) throw new NetFlowException("Channel closed")
    }

    val channel = sk.channel().asInstanceOf[SocketChannel]
    val holder = sk.attachment().asInstanceOf[PacketHolder]

    try {
      if (holder.content != null) {
        // reading packet
        val curContent = holder.content
        channelRead(channel, curContent)
        if (!curContent.hasRemaining) {
          dealWithCommand(channel, curContent)
          holder.content = null
          holder.len.clear()
        }
      } else if (holder.len.position < 2) {
        // reading length first
        val lb = holder.len
        channelRead(channel, lb)
        if (lb.position == 2) {
          val packetLength = lb.getShort(0)
          if (packetLength < 0) {
            throw new NetFlowException("Invalid packet length")
          }
          holder.content = ByteBuffer.allocate(packetLength - 2)
          readPacketFromSocket(sk)
        }
      }
    } catch {
      case e: NetFlowException =>
        logWarning(s"Error occurs while reading packets: ${e.getMessage}")
        closeSocketChannel(sk)
      case e : Exception => logError(e.getMessage)
    }
  }

  private def closeSocketChannel(key: SelectionKey): Unit = {

    // When a channel is closed, all keys associated with it are automatically cancelled


    key.attach(null)
    key.cancel()
    val c = key.channel()
    val collectorIP = getRemoteIp(c.asInstanceOf[SocketChannel])
    c.close()
    channels -= c

    ipToChannel -= collectorIP
    master! DeleReceiver(collectorIP)
  }

  private def dealWithCommand(curChannel: SocketChannel, data: ByteBuffer): Unit = {
    if (CommandSet.isReqWorkerList(data)) {

      // delete dead worker
      CommandSet.getDeadWorker(data) match {
        case Some(result) => master ! DeleWorker(result._1, result._2)
        case None =>
      }
      master ! RequestWorker(getRemoteIp(curChannel))
    }
  }

  private class PacketHolder {
    val len: ByteBuffer = ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN)
    var content: ByteBuffer = _
  }
}

object CommandSet {

  /* the command struct that connect with receiver */
  object CommandStruct {
    // the command request from worker to master
    val req_workerlist = "$req$"
    val req_report = "$report$"

    // the command response from master to worker
    val res_prefix = "$$"

    // inner command definition
    val delim = "&"
  }


  private val req_buffer = ByteBuffer.allocate(1500)
  private val res_buffer = ByteBuffer.allocate(1500)

  private val sb = new StringBuilder

  /**
   *    $$+2&1.2.3.4:1000&2.2.2.2:1234&
   *    $$-1&3.4.5.6:1000&
   *    $$-1&3.4.5.6:1000&+2&1.2.3.4:1000&2.2.2.2:1234&
   * @return
   */
  def responseReceiver(
                        addIpAdds: Option[Array[(String, Int)]],
                        deleIpAdds: Option[Array[(String, Int)]]): ByteBuffer = {
    res_buffer.clear()
    sb.clear()
    sb.append(CommandStruct.res_prefix) // $$

    deleIpAdds match {
      case Some(ipAdds) =>
        sb.append("-").append(ipAdds.length).append(CommandStruct.delim)
        ipAdds.map(ip_port => {
          sb.append(ip_port._1).append(":").append(ip_port._2).append(CommandStruct.delim)
        })
      case None =>
    }

    addIpAdds match {
      case Some(ipAdds) =>
        sb.append("+").append(ipAdds.length).append(CommandStruct.delim)
        ipAdds.map(ip_port => {
          sb.append(ip_port._1).append(":").append(ip_port._2).append(CommandStruct.delim)
        })
      case None =>
    }

    res_buffer.put(sb.toString().getBytes)
    res_buffer.flip()
    res_buffer
  }

  def isReqWorkerList(data: ByteBuffer): Boolean = {
    data.array.startsWith(CommandStruct.req_workerlist)
  }

  def getDeadWorker(data: ByteBuffer): Option[(String, Int)] = {
    if (data.limit() == CommandStruct.req_workerlist.length) {
      None
    } else {
      val pos = CommandStruct.req_workerlist.length
      assert(data.get(pos) == '-')
      data.position(pos + 1)
      val ipStr = new String(data.array(), data.position(), data.remaining()).split(":")
      assert(ipStr.length == 2) // only has one address
      Some((ipStr(0), ipStr(1).toInt))
    }
  }

  def getReqBuffer: ByteBuffer = {
    req_buffer.clear()
    req_buffer
  }
}
