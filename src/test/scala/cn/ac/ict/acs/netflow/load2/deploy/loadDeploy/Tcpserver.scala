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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.ac.ict.acs.netflow.load2.deploy.loadDeploy

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{SocketChannel, ServerSocketChannel, Selector, SelectionKey}

/**
 * Created by ayscb on 2015/5/27.
 */
object Tcpserver {

  val selector = Selector.open()
  val sendBuff = ByteBuffer.allocate(1500)
  val recvBuff = ByteBuffer.allocate(1500)

  def main(args: Array[String]) {
    // start service socket
    val serverSocketChannel = java.nio.channels.ServerSocketChannel.open()
    serverSocketChannel.configureBlocking(false)
    serverSocketChannel.socket().bind(new InetSocketAddress(10012))

    serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT)
    println("Server Start on 10012")

    while (!Thread.interrupted()) {
      if (selector.select() != 0) {
        val iter = selector.selectedKeys().iterator()
        while (iter.hasNext) {
          val key = iter.next()
          iter.remove()

          if (key.isAcceptable) {
            acceptConnection(key)
          } else if (key.isReadable) {
            readConnection(key)
          } else if (key.isWritable) {
            writeConnection(key)
            println("--------------------------")
          }
        }
      }
    }
    selector.close()
    printf("[ netflow ]The Service for Receiver is closed. ")
  }

  private def acceptConnection(key: SelectionKey): Unit = {
    // connect socket
    val socketChannel = key.channel().asInstanceOf[ServerSocketChannel].accept()
    socketChannel.configureBlocking(false)
    socketChannel.register(selector, SelectionKey.OP_READ)

    // save the remote ip
    val remoteIP = socketChannel.getRemoteAddress.asInstanceOf[InetSocketAddress]
      .getAddress.getHostAddress
    println(s"[ netFlow ] The collect Service accepts a connection from $remoteIP. ")
  }

  private def readConnection(key: SelectionKey): Unit = {
    val channel = key.channel().asInstanceOf[SocketChannel]
    val remoteHost =
      channel.getRemoteAddress.asInstanceOf[InetSocketAddress]
        .getAddress.getHostAddress
    println("sssss")
    recvBuff.clear()
    val count = channel.read(recvBuff)
    if (count > 0) {
       println(new String(recvBuff.array()))
      recvBuff.flip()
      sendBuff.put(recvBuff)
      key.interestOps(SelectionKey.OP_WRITE)
    }
  }

  private def writeConnection(key: SelectionKey): Unit = {
    val channel = key.channel().asInstanceOf[SocketChannel]
    sendBuff.flip()
    channel.write(sendBuff)
    sendBuff.clear()
    key.interestOps(SelectionKey.OP_READ)
  }
}
