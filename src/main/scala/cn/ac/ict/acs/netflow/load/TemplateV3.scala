package cn.ac.ict.acs.netflow.load

import org.apache.spark.sql.catalyst.expressions.GenericMutableRow

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Random

/**
 * ues array list to stand for the hash map
 * Created by ayscb on 2015/4/16.
 */
object TemplateV3 {
  val BYTE_MAX = 255
  val SHORT_MAX = 65535
  val INT_MAX = 4294967295L
  val IP_MAX = 256

  private val keyList = new ArrayBuffer[Int]()
  private val valueList = new ArrayBuffer[Int]()

  /** get the length from **/
  private var rowLength = 0

  /**
   * template
   *
   * @param fileName template  file path
   */
  def load(fileName: String): TemplateV3 = {

    val file = Source.fromFile(fileName)
    if (file == null) {
      throw new java.lang.IllegalAccessError(
        String.format("the file %s does not exist!", fileName))
    }

    val lineIter = file.getLines()

    for (line <- lineIter) {
      val kv: Array[String] = line.split(" ")
      val valueLen = kv(1).toInt
      keyList += kv(0).toInt
      valueList += valueLen
      rowLength += valueLen
    }

    file.close()
    TemplateV3(keyList,valueList, rowLength)
  }
}

case class TemplateV3(keyList:ArrayBuffer[Int], valueList : ArrayBuffer[Int], rowBytes: Int) {

    @transient lazy val rd: Random = new Random()

    val LongKey = Set(1, 2, 3, 10, 14, 16, 17, 19, 20, 23, 24, 40, 41, 42)
    val variableKey = Set( 82, 83, 84, 90, 94, 95, 96, 100)
    // we regard  the ip mac type as var length type
    val variableKeys = variableKey ++ Set(8, 12, 15, 18, 27, 28, 47, 56, 57, 62, 63, 80, 81)

    def getRowLength = rowBytes

    def getRowData(currTime: Long,
                   row: GenericMutableRow
                    ) = {

      // rd = if (rd == null) new Random(currTime) else rd
      if (keyList.size == 0)
        throw new java.lang.RuntimeException("should call load first")

      row.setLong(0, currTime) // time

      for( i <- 0 until keyList.size) {
        val key = keyList(i)
        val valLen = valueList(i)

        if( variableKeys.contains(key)) {
          setVarDataLength(key, valLen, row)
        }else {
          setFixeLengthData(key, valLen, row)
        }
      }
    }

    private def setFixeLengthData(key: Int, valueLen: Int, row: GenericMutableRow): Unit = {
      if( LongKey.contains(key)){
        val v = Math.abs(rd.nextLong() % TemplateV3.INT_MAX)
        row.setLong(key, v)
        return
      }
      valueLen match {
        case 1 =>
          val v = rd.nextInt(TemplateV3.BYTE_MAX).toShort
          row.setShort(key, v)

        case 2 =>
          val v = rd.nextInt(TemplateV3.SHORT_MAX)
          row.setInt(key, v)

        case 4 =>
          val v = Math.abs(rd.nextLong() % TemplateV3.INT_MAX)
          row.setLong(key, v)
      }
    }

    private def setVarDataLength(key: Int, valueLen: Int,
                                 row: GenericMutableRow): Unit = {
      val value = key match {
        case (8 | 12 | 15) => getIPV4 // ipv4
        case (27 | 28 | 62 | 63) => getIPV6 // ipv6
        case (56 | 57 | 80 | 81) => getMAC // mac
        case _ => getStringDataLength(valueLen)
      }
      row.update(key,value)
    }

    private def getStringDataLength(dataLen: Int): Array[Byte] = {
      val result = new Array[Byte](dataLen)
      rd.nextBytes(result)
      result
    }

    private def getIPV4 = getStringDataLength(4)

    // 16个字节 分8组 (  FE80:0000:0000:0000:AAAA:0000:00C2:0002 )
    private def getIPV6 = getStringDataLength(16)

    // 6个字节 表示的是  00-23-5A-15-99-42
    private def getMAC = getStringDataLength(6)
}
