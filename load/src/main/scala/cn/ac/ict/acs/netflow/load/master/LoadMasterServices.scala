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

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{ServerSocketChannel, SelectionKey, Selector, SocketChannel}

import scala.collection.mutable.ArrayBuffer

import cn.ac.ict.acs.netflow.util.Utils

trait MasterService {
  self: LoadMaster =>

  // get the tcp thread runner
  private var Service: Thread = null
  private var ActualPort: Int = 0

  def StopThread() = if (Service != null) Service.interrupt()
  def getActualPort = ActualPort

  def startThread() = {
    val receiverPort = conf.getInt("netflow.receiver.port", 10012)
    val t = Utils.startServiceOnPort(receiverPort, doStartRunner, conf, "Receiver-Master")
    Service = t._1
    ActualPort = t._2
  }

  private def doStartRunner(Port: Int): (Thread, Int) = {
    val thread =
      new Thread("Receiver-Master-Service") {
        logInfo(s"[ netflow ] The Service for Receiver is ready to start ")
        private val selector = Selector.open()

        override def run(): Unit = {
          logInfo(s"[ netflow ] The Service for Receiver is running on port $Port ")

          // start service socket
          val serverSocket = java.nio.channels.ServerSocketChannel.open()
          serverSocket.configureBlocking(false)
          serverSocket.bind(new InetSocketAddress(Port))
          serverSocket.register(selector, SelectionKey.OP_ACCEPT)

          while (!Thread.interrupted()) {
            if (selector.select() != 0) {
              val iter = selector.selectedKeys().iterator()
              while (iter.hasNext) {
                val key = iter.next()
                if (key.isAcceptable) {
                  addSubConnection(key)
                } else if (key.isReadable) {
                  dealWithReadSubConnection(key)
                } else if (key.isConnectable) {
                  logInfo("--------------------------")
                }
                iter.remove()
              }
            }
          }
          selector.close()
          logInfo(s"[ netflow ]The Service for Receiver is closed. ")
        }

        /* deal with connection from remote */
        private def addSubConnection(key: SelectionKey): Unit = {
          // connect socket
          val socketChannel = key.channel().asInstanceOf[ServerSocketChannel].accept()
          socketChannel.configureBlocking(false)
          socketChannel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE)

          // save the remote ip
          val remoteIP = socketChannel.getRemoteAddress.asInstanceOf[InetSocketAddress]
            .getAddress.getHostAddress
          registerReceiverStruct(remoteIP, socketChannel)
          logInfo(s"[ netFlow ] The collect Service accepts a connection from $remoteIP. ")
        }

        import NetUtil.Mode._

        // deal with the read connection request
        private def dealWithReadSubConnection(key: SelectionKey): Unit = {
          val channel = key.channel().asInstanceOf[SocketChannel]
          val remoteHost =
            channel.getRemoteAddress.asInstanceOf[InetSocketAddress]
              .getAddress.getHostAddress
          logInfo(s"[Netflow] The receiver $remoteHost is connect to master.")

          val buff = NetUtil.getBuffer
          val count = channel.read(buff)
          if (count > 0) {
            buff.flip()
            // prase buffer
            if (buff.array.startsWith("$request$")) {
              val data = assignWorker(remoteHost)
              if (data != None) {
                channel.write(NetUtil.responseReceiver(add, data.get))
              }
            }
          }
          channel.register(selector, SelectionKey.OP_READ)
        }

        // the method is called only by first connection for receiverIP
        private def registerReceiverStruct(receiverIP: String, socketChannel: SocketChannel) = {
          if (receiverToSocket.contains(receiverIP)) {
            logError(s"[ netflow ] The receiver $receiverIP should not exist in receiverToSocket !")
          }
          if (receiverToWorkers.contains(receiverIP)) {
            logError(s"[ netflow ] The receiver $receiverIP " +
              s"should not exist in receiverToWorkers !")
          }

          receiverToSocket += (receiverIP -> socketChannel)
          receiverToWorkers += (receiverIP -> new ArrayBuffer[String]())
        }

        // called when a receiver ask for worker's info
        private def assignWorker(receiver: String, workerNum: Int = 1):
          Option[Array[(String, Int)]] = {

          receiverToWorkers.get(receiver) match {
            case None =>
              logError(s"[ Netflow ] The node $receiver should be registed! ")
              None
            case Some(workers) =>
              if (workers.size == 0) {
                if (!workerToPort.contains(receiver)) Thread.sleep(2000)
                if (workerToPort.contains(receiver)) {
                  workers += receiver
                }

                if (workerNum != 1) {
                  val orderByworkerList = workerToBufferRate.iterator.toList.sortWith(_._2 < _._2)

                  var oi = 0
                  while (workers.size != workerNum) {
                    val host = orderByworkerList(oi)._1
                    workers += host
                    workerToReceivers.get(host).get += receiver
                    oi = (oi + 1) % orderByworkerList.size
                  }
                }
              }

              val workerList = new Array[(String, Int)](workerNum)
              for (i <- 0 until workers.size) {
                workerList(0) = workerToPort.get(workers(i)).get
              }
              Some(workerList)
          }
        }
      }

    thread.start()
    (thread, Port)
  }
}

object NetUtil {

  private val buffer = ByteBuffer.allocate(1500)

  object Mode extends Enumeration {
    type Mode = Value
    val add, delete = Value
  }

  import Mode._
  /**
   *
   * @param mode
   * + add new ips
   * - delete exist ips
   * @param ipAdds
   * @return
   * if the ipAdds is available, return ips
   * else return "null"
   */
  def responseReceiver(mode: Mode, ipAdds: Array[(String, Int)]): ByteBuffer = {
    buffer.clear()
    if (mode == add) {
      val str = "$$$+" +
        ipAdds.length.toString + "&" +
        ipAdds.map(ip => ip._1.concat(":" + ip._2)).mkString("&")
      buffer.put(str.getBytes)
    } else if (mode == delete) {
      val str = "$$$-" +
        ipAdds.length.toString + "&" +
        ipAdds.map(ip => ip._1.concat(":" + ip._2)).mkString("&")
      buffer.put(str.getBytes)
    }
    buffer
  }

  def getBuffer: ByteBuffer = {
    buffer.clear()
    buffer
  }
}
