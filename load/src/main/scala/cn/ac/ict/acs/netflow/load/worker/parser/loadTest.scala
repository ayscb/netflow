package cn.ac.ict.acs.netflow.load.worker.parser

import java.io.{DataInputStream, FileInputStream}
import java.nio.ByteBuffer

import cn.ac.ict.acs.netflow.NetFlowException
import cn.ac.ict.acs.netflow.load.worker.Row

/**
 * Created by ayscb on 15-6-13.
 */
object loadTest {

  val data = ByteBuffer.allocate(1550)

  val ip = Array(
    192.asInstanceOf[Byte],
    168.asInstanceOf[Byte],
    1.asInstanceOf[Byte],
    1.asInstanceOf[Byte]
  )
  data.put(4.asInstanceOf[Byte])
  data.put(ip)

  val pos = data.position()

  def main(args: Array[String]) {
    var idx = 0

    var packageCount = 0
    var rowCount = 0

    var fileCount = 0
    val s = System.currentTimeMillis()

    while(idx != args.length){
      val file = args(idx)
      val fi = new FileInputStream(file)
      val bf = new DataInputStream(fi)

      fileCount += bf.available()
      while(bf.available() != 0) {
        val length = bf.readShort()
        data.position(pos)
        bf.read(data.array(), data.position(), length)
        val limit = data.position() + length

        data.flip()
        data.limit(limit)

        try{
          packageCount += 1
          val (flowSets, packetTime) = PacketParser.parse(data)
          val rows: Iterator[Row] = flowSets.flatMap(_.getRows)
          while(rows.hasNext){
            val row = rows.next()
        //    println(s"package's limit : ${row.bb.limit()}, row's start: ${row.startPos}, row's length: ${row.template.rowLength}" )
            rowCount += 1
          }
        }catch {
          case e :NetFlowException => // println(e.getMessage)
        }
      }
      idx += 1
    }
    println(s" package count: $packageCount  rowCount: $rowCount")
    println(s"file : ${fileCount/1024/1024}MB --time: ${System.currentTimeMillis() - s}ms})")
  }
}
