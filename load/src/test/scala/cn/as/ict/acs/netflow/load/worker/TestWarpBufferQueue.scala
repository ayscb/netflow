package cn.as.ict.acs.netflow.load.worker

import java.nio.ByteBuffer

import cn.ac.ict.acs.netflow.load.worker.WrapBufferQueue
import org.scalatest.FunSuite

/**
 * Created by ayscb on 15-6-19.
 */
class TestWarpBufferQueue extends FunSuite{

  def loadBalanceStrategyFunc() : Unit = {
    println("loadBalanceStrategyFunc")
  }

  def sendOver(): Unit={
    println("sendOver")
  }

  test("test fun call"){
    val warper = new WrapBufferQueue(100,70,loadBalanceStrategyFunc, sendOver)
    for(i <- 0 until 70){
      assert(warper.size == i)
      warper.put(ByteBuffer.allocate(10))
    }

    for(i <- 70 until 100){
      assert(warper.size == i)
      warper.put(ByteBuffer.allocate(10))
    }
  }
}
