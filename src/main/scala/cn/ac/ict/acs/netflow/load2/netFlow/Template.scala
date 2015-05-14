package cn.ac.ict.acs.netflow.load2.netFlow

import java.nio.ByteBuffer
import java.util.concurrent.locks.ReentrantReadWriteLock
import cn.ac.ict.acs.netflow.load2.parquetUtil.NetFlowShema
import scala.collection.mutable.ArrayBuffer

/**
 * a template for record valid fields
 * Created by ayscb on 2015/4/13.
 */

abstract class Template( val tmpId : Int, val fieldsCount : Int ) extends cloneable with Iterable[(Int,Int)]{
  protected var recordBytes = 0
  protected val keyList = new ArrayBuffer[Int](fieldsCount)
  protected val valueList = new ArrayBuffer[Int](fieldsCount)

  def updateTemplate(id : Int, fieldsNum : Int, data : ByteBuffer) : Unit
  def getTemplate : ((Array[Int], Array[Int]), Int)     // ( key, value, byteCount )
  def getTemplate1 : Template

  override def iterator: Iterator[(Int,Int)] = new Iterator[(Int,Int)]{
    private var currId = 0
    override def hasNext: Boolean = currId < fieldsCount

    override def next(): (Int, Int) =
      if( hasNext ) {
        val nxt = (keyList(currId),valueList(currId))
        currId += 1 ;
        nxt
      }else
        throw new NoSuchElementException("next on empty iterator")
  }
}

class UnSafeTemplate ( tmpId : Int, fieldsCount :Int ) extends Template(tmpId,fieldsCount) {

  override def getTemplate: ((Array[Int], Array[Int]), Int) = {
    ((keyList.toArray, valueList.toArray), recordBytes)
  }

  override def getTemplate1 : Template = this

  override def updateTemplate(id: Int, fieldsNum: Int, data: ByteBuffer): Unit = {
    if (id != tmpId) {
      throw new RuntimeException("[ error ! " +
        "The template ID should be same for the same template." +
        " But now expect ID = " + tmpId + " and the real id = " + id)
    }

    //TODO : how do we judge the same template ? only by compare the fields count ?
    if (fieldsNum == fieldsCount) {
      // skip the template length
      data.position(data.position() + fieldsCount * 4)
      return
    }

    keyList.clear()
    valueList.clear()

    for (i <- 0 until fieldsCount) {
      val key = NetFlowShema.mapKey2Clm(BytesUtil.toUShort(data))
      val valueLen = BytesUtil.toUShort(data)

      if (key == -1) {
        keyList.clear()
        valueList.clear()
        return
      }

      valueList += valueLen
      keyList += key
      recordBytes += valueLen
    }
  }
}

class SafeTemplate( tmpId :Int, fieldsCount :Int) extends Template(tmpId,fieldsCount){

  private val wrl = new ReentrantReadWriteLock()
  private val readLock = wrl.readLock()
  private val writeLock = wrl.writeLock()

  override def getTemplate: ((Array[Int], Array[Int]), Int) = {
    readLock.lock()
    try {
      ((keyList.toArray, valueList.toArray), recordBytes)
    }
    finally {
      readLock.unlock()
    }
  }

  override def getTemplate1 : Template = {
    readLock.lock()
    try{
      this.clone().asInstanceOf[SafeTemplate]
    }finally {
      readLock.unlock()
    }
  }

  /**
   * update the template when it exists and new one when is does not exists
   * @param id tmpID , only for check
   * @param data the netflow package data
   */
  override def updateTemplate(id: Int, fieldsNum: Int, data: ByteBuffer): Unit = {

    writeLock.lock()
    try {
      if (id != tmpId) {
        throw new RuntimeException("[ error ! " +
          "The template ID should be same for the same template." +
          " But now expect ID = " + tmpId + " and the real id = " + id)
      }

      //TODO : how do we judge the same template ? only by compare the fields count ?
      if (fieldsNum == fieldsCount) {
        // skip the template length
        data.position(data.position() + fieldsCount * 4)
        return
      }

      keyList.clear()
      valueList.clear()

      for (i <- 0 until fieldsCount) {
        val key = NetFlowShema.mapKey2Clm(BytesUtil.toUShort(data))
        val valueLen = BytesUtil.toUShort(data)

        if (key == -1) {
          keyList.clear()
          valueList.clear()
          return
        }

        valueList += valueLen
        keyList += key
        recordBytes += valueLen
      }
    } finally {
      writeLock.unlock()
    }
  }
}
