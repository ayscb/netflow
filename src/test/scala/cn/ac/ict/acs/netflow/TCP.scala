package cn.ac.ict.acs.netflow

import java.net.InetSocketAddress
import java.nio.channels.{ServerSocketChannel, SelectionKey, Selector}

/**
 * Created by ayscb on 2015/5/8.
 */
object TCP extends Logging {

  val collectPort = 10012

  val collectService : Thread = new Thread ( " collectService ") {
    logInfo(s"[ netflow ] The collect Service is ready to start. ")
    private val selector = Selector.open()

    override def run(): Unit = {

      logInfo(s"[ netflow ] The collect Service is running on port $collectPort ")

      // start service socket
      val serverSocket = java.nio.channels.ServerSocketChannel.open()
      serverSocket.configureBlocking(false)
      serverSocket.bind(new InetSocketAddress(collectPort))
      serverSocket.register(selector, SelectionKey.OP_ACCEPT)

      while (!Thread.interrupted()) {
        if (selector.select() != 0) {
          val iter = selector.selectedKeys().iterator()
          while (iter.hasNext) {
            val key = iter.next()
            if (key.isAcceptable) {
              val channel = key.channel().asInstanceOf[ServerSocketChannel]
              val socketChannel = channel.accept()
              socketChannel.configureBlocking(false)
              socketChannel.register(selector,SelectionKey.OP_READ)
              key.interestOps(SelectionKey.OP_ACCEPT)
              // addSubConnection(key)
            } else if (key.isReadable) {
              // dealWithReadSubConnection(key)
              logInfo("-----------------ddddd---------")
              key.interestOps(SelectionKey.OP_READ)
            } else if (key.isConnectable) {
              logInfo("--------------------------")
            }
            iter.remove()
          }
        }
      }
      selector.close()
      logInfo(s"[ netflow ] The collect Service is closed. ")
    }

    private def addSubConnection(key: SelectionKey): Unit = {
      val socketChannel = key.channel().asInstanceOf[ServerSocketChannel].accept() // connect socket
      socketChannel.configureBlocking(false)
      socketChannel.register(selector, SelectionKey.OP_READ )
//      val remoteIP = socketChannel.getRemoteAddress.asInstanceOf[InetSocketAddress]
//        .getAddress.getHostAddress
//      // registerCollectorStruct( remoteIP, socketChannel )
//      logInfo(s"[ netFlow ] The collect Service accepts a connection from $remoteIP. ")
    }
  }

  def main (args: Array[String]) {
    collectService.setDaemon(false)
    collectService.start()
    Thread.sleep(50000)
    collectService.interrupt()
  }
}
