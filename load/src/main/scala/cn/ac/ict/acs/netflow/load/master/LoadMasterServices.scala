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
import cn.ac.ict.acs.netflow.load2.deploy.loadDeploy.LoadMaster

import scala.collection.mutable.ArrayBuffer

trait MasterService {
  self: LoadMaster =>

  // get the tcp thread runner
  private var Service: Thread = null
  private var ActualPort: Int = 0

  def StopThread() = if (Service != null) Service.interrupt()
  def getActualPort = ActualPort

  def startMasterService() = {
    val receiverPort = conf.getInt("netflow.receiver.port", 10012)
    val t = Utils.startServiceOnPort(receiverPort, doStartRunner, conf, "Receiver-Master")
    Service = t._1
    ActualPort = t._2
  }

  private def doStartRunner(Port: Int): (Thread, Int) = {
    val thread =
      new Thread("Receiver-Master-Service") {

        logInfo(s"[Netflow] The Service for Receiver is ready to start ")
        private val selector = Selector.open()

        override def run(): Unit = {
          logInfo(s"[Netflow] The Service for Receiver is running on port $Port ")

          // start service socket
          val serverSocket = java.nio.channels.ServerSocketChannel.open()
          serverSocket.configureBlocking(false)
          serverSocket.socket().bind(new InetSocketAddress(Port))
          serverSocket.register(selector, SelectionKey.OP_ACCEPT)

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
                 // writeConnection(key)
                }else if( key.isValid){
                  printf("valid")
                }else if(key.isConnectable){
                  printf("isConnectable")
                }
              }
            }
          }
          selector.close()
          logInfo(s"[Netflow] The Service for Receiver is closed. ")
        }

        /* deal with connection from remote */
        private def acceptConnection(key: SelectionKey): Unit = {
          // connect socket
          val socketChannel = key.channel().asInstanceOf[ServerSocketChannel].accept()
          socketChannel.configureBlocking(false)
          socketChannel.register(selector, SelectionKey.OP_READ)

          // save the remote ip
          val remoteIP = socketChannel.getRemoteAddress.asInstanceOf[InetSocketAddress]
            .getAddress.getHostAddress
          registerReceiverStruct(remoteIP, socketChannel)

          logInfo(s"[Netflow] The receiver Service accepts a connection from $remoteIP.receiver ")
        }


        // deal with the read connection request
        private def readConnection(key: SelectionKey): Unit = {
          val channel = key.channel().asInstanceOf[SocketChannel]
          val remoteHost =
            channel.getRemoteAddress.asInstanceOf[InetSocketAddress]
              .getAddress.getHostAddress

          logInfo(s"[Netflow] The $remoteHost receiver is request to master.")

          val buff = NetUtil.getBuffer
          val count = channel.read(buff)
          if (count > 0) {
            buff.flip()
            if (buff.array.startsWith("$req$")){
              val prefix_len = "$req$".length
              if(buff.limit() != prefix_len && buff.get(prefix_len) == '-'){
                // get dead ip
                buff.position(prefix_len+1)
                val ips = new Array[Byte](40)
                val len = Math.min(40,buff.remaining())
                buff.get(ips, 0, len)
                val ipStr = new String(ips,0,len).split(":")
                if( workerToPort.contains(ipStr(0))){
                  if( workerToPort(ipStr(0))._2 == ipStr(1).toInt){
                    workerToPort -= ipStr(0)
                    workerToBufferRate -= ipStr(0)
                    workerToReceivers -= ipStr(0)
                    receiverToWorkers(remoteHost)-= ipStr(0)
                  }
                }
              }

              // get worker ips
              val data = assignWorker(remoteHost)
              if (data != None) {
                  channel.write(netUtil.responseReceiver(Mode.add, data.get))
              }else{
                  // No available worker to assigned
                  waitQueue += (remoteHost -> channel )
              }
            }

            channel.register(selector, SelectionKey.OP_READ)
          }else if(count == -1){
            // close socket
            channel.shutdownInput()
            receiverToSocket -= remoteHost
            receiverToWorkers -= remoteHost
            workerToReceivers.foreach(x=>{
              x._2 -= remoteHost
            })
            key.cancel()
            logInfo(s"[Netflow] The $remoteHost receiver is closed")
          }
        }

        // the method is called only by first connection for receiverIP
        private def registerReceiverStruct(receiverIP: String, socketChannel: SocketChannel) = {
          if (receiverToSocket.contains(receiverIP)) {
            logError(s"[Netflow] The receiver $receiverIP should not exist in receiverToSocket !")
          }
          if (receiverToWorkers.contains(receiverIP)) {
            logError(s"[Netflow] The receiver $receiverIP should not exist in receiverToWorkers !")
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
                // the first time to assign worker

                if( workerToPort.size == 0 ){
                  logWarning(s"[Netflow] There is no available worker to run in cluster.")
                  return None
                }

                // first, try to assign itself
                if (!workerToPort.contains(receiver)) Thread.sleep(2000)
                if (workerToPort.contains(receiver)) {
                  workers += receiver
                }

                // if needs more than one worker
                if (workers.size != workerNum) {

                  val orderByworkerList = workerToBufferRate.filterNot(x=>x._1 == receiver)
                    .toList.sortWith(_._2 < _._2)

                  var oi = 0
                  while (workers.size != workerNum) {
                    val host = orderByworkerList(oi)._1
                    workers += host
                    oi = (oi + 1) % orderByworkerList.size
                  }
                }
              }

              val workerList = new Array[(String, Int)](workerNum)
              for (i <- 0 until workers.size) {
                workerList(i) = workerToPort.get(workers(i)).get
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
