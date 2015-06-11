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

package cn.ac.ict.acs.netflow.load.worker

import java.io.IOException
import java.net.{ InetSocketAddress, ServerSocket }
import java.nio.ByteBuffer
import java.nio.channels._

import scala.collection.mutable

import cn.ac.ict.acs.netflow.util.Utils
import cn.ac.ict.acs.netflow.{ NetFlowException, Logging, NetFlowConf }

/**
 * A multi-way receiver that serve connections from collectors and read packets from it
 * @param packetBuffer buffer gathering incoming packets
 * @param conf netflowConf
 *
 * TODO thread restart?
 */
class Receiver(packetBuffer: WrapBufferQueue, conf: NetFlowConf) extends Runnable with Logging {

  logInfo(s"Initializing Multi-Way Receiver")
  private var selector: Selector = _

  val channels = mutable.HashSet.empty[Channel]
  val channelToIp = mutable.HashMap.empty[Channel, String]

  def collectors = channelToIp.values

  override def run() = {
    val serverSocketChannel = ServerSocketChannel.open()
    serverSocketChannel.configureBlocking(false)
    val serverSocket = serverSocketChannel.socket()
    channels += serverSocketChannel

    val (_, actualPort) =
      Utils.startServiceOnPort(0, startListening(serverSocket), conf, "multi-way receiver")
    logInfo(s"Receiver is listening $actualPort for netflow packets")

    selector = Selector.open()
    serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT)

    try {
      while (true) {
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
          case e: IOException =>
            // TODO
        }
      }
    } finally {
      // When a selector is closed, all channels registered with that selector are deregistered,
      // and the associated keys are invalidated(cancelled)
      logWarning("multi-way receiver terminated")
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

  /**
   * Accept remote connection
   */
  private def registerChannel(sk: SelectionKey): Unit = {
    // connect socket
    val socketChannel = sk.channel().asInstanceOf[ServerSocketChannel].accept()
    socketChannel.configureBlocking(false)
    socketChannel.register(selector, SelectionKey.OP_READ).attach(new PacketHolder)

    val remoteIP = socketChannel.getRemoteAddress.asInstanceOf[InetSocketAddress]
      .getAddress.getHostAddress
    channels += socketChannel
    channelToIp(socketChannel) = remoteIP
    logInfo(s"Open connection from $remoteIP.")
  }

  /**
   * Read at most 1 packet from the connection
   */
  private def readPacketFromSocket(sk: SelectionKey): Unit = {
    def channelRead(channel: SocketChannel, buffer: ByteBuffer): Unit = {
      if (channel.read(buffer) < 0) throw new NetFlowException("Channel closed")
    }

    val channel = sk.channel().asInstanceOf[SocketChannel]
    val holder = sk.attachment().asInstanceOf[PacketHolder]

    try {
      if (holder.content != null) { // reading packet
        val curContent = holder.content
        channelRead(channel, curContent)
        if (curContent.position == curContent.capacity) {
          packetBuffer.put(curContent)
          holder.content = null
          holder.len.clear()
        }
      } else if (holder.len.position < 2) { // reading length first
        val lb = holder.len
        channelRead(channel, lb)
        if (lb.position == 2) {
          val packetLength = lb.getShort(0)
          if (packetLength < 0) {
            throw new NetFlowException("Invalid packet length")
          }
          holder.content = ByteBuffer.allocate(packetLength)
          readPacketFromSocket(sk)
        }
      }
    } catch {
      case e: NetFlowException =>
        logWarning(s"Error occurs while reading packets: ${e.getMessage}")
        close(sk)
    }
  }

  private def close(key: SelectionKey): Unit = {
    // When a channel is closed, all keys associated with it are automatically cancelled
    key.attach(null)
    val c = key.channel()
    channels -= c
    channelToIp.remove(c)
    c.close()
  }

  class PacketHolder {
    val len: ByteBuffer = ByteBuffer.allocate(2)
    var content: ByteBuffer = _
  }
}
