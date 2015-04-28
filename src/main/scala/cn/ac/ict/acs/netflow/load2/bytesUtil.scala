package cn.ac.ict.acs.netflow.load2

/**
 * Created by ayscb on 2015/4/13.
 */
object bytesUtil {

  def toUShort(value:Array[Byte],offset:Int,length:Int) : Int = {
    if(length != 2 )
      throw  new IllegalArgumentException("the int length should be 4 ," +
        " but now is " + length)
    (value(0) & 0XFF << 8) |
    (value(1) &0XFF << 0)
  }

  def toUInt(value :Array[Byte],offset:Int,length:Int): Long = {
    if(length != 4 )
      throw  new IllegalArgumentException("the int length should be 4 ," +
        " but now is " + length)
    ((value(0) & 0xFFL)<< 24) |
    ((value(0) & 0xFFL)<< 16) |
    ((value(0) & 0xFFL)<< 8)  |
    ((value(0) & 0xFFL)<< 0)
  }

  def toBytes(value :Short) : Array[Byte]= {
    val tmp = new Array[Byte](2)
    tmp(1) = ((value >>> 0) & 0xFF).asInstanceOf[Byte]
    tmp(0) = ((value >>> 8) & 0xFF ).asInstanceOf[Byte]
    tmp
  }

  def toBytes(value :Int) : Array[Byte]= {
    val tmp = new Array[Byte](4)
    tmp(3) = ((value >>> 0) & 0xFF ).asInstanceOf[Byte]
    tmp(2) = ((value >>> 8) & 0xFF ).asInstanceOf[Byte]
    tmp(1) = ((value >>> 16) & 0xFF).asInstanceOf[Byte]
    tmp(0) = ((value >>> 24) & 0xFF ).asInstanceOf[Byte]
    tmp
  }

  def putByte(buff:Array[Byte] , offset : Int, length : Int, value:Short): Unit ={
    buff(1) = ((value >>> 0) & 0xFF).asInstanceOf[Byte]
    buff(0) = ((value >>> 8) & 0xFF ).asInstanceOf[Byte]
  }

  def putByte(buff:Array[Byte] , offset : Int, length : Int, value:Int):Unit = {
    buff(3) = ((value >>> 0) & 0xFF ).asInstanceOf[Byte]
    buff(2) = ((value >>> 8) & 0xFF ).asInstanceOf[Byte]
    buff(1) = ((value >>> 16) & 0xFF).asInstanceOf[Byte]
    buff(0) = ((value >>> 24) & 0xFF ).asInstanceOf[Byte]
  }

  def main(args: Array[String]) {

    // short to bytes and bytes to short
    val s1=0
    val s2 = 10
    val s3 = 55531
    val shortval = bytesUtil.toBytes(s3.asInstanceOf[Short])
    println(bytesUtil.toUShort(shortval,0,shortval.length))

    // int to bytes
    val i1 = 0
    val i2 = Int.MaxValue
    val i3 : Long = ( Int.MaxValue + 1 ) * 2 - 1

    val intval = bytesUtil.toBytes(i3.asInstanceOf[Int])
    print( bytesUtil.toUInt(intval,0,intval.length ))
  }
}
