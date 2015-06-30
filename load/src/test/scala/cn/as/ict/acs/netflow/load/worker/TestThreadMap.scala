package cn.as.ict.acs.netflow.load.worker

import scala.collection.mutable

/**
 * Created by ayscb on 6/29/15.
 */
object TestThreadMap {

  val writeThreadRate = new mutable.HashMap[Thread, String]()

  def t : Thread = {
    new Thread( new Runnable {
      //   writeThreadRate(Thread.currentThread()) = Thread.currentThread().getName + Thread.currentThread().getId
      override def run(): Unit = {
        writeThreadRate(Thread.currentThread()) = Thread.currentThread().getName + Thread.currentThread().getId
        var i = 10
        while(true){
          writeThreadRate(Thread.currentThread()) = writeThreadRate(Thread.currentThread()).concat(" -- " +i.toString)
       //   assert(Thread.currentThread().getName + Thread.currentThread().getId == writeThreadRate(Thread.currentThread()))
          println(Thread.currentThread().getName + Thread.currentThread().getId +"---->"+ writeThreadRate(Thread.currentThread()))
          Thread.sleep(500)
          i -= 1
        }
      }
    })
  }

  def main(args: Array[String]) {
    for(i <-0 to 100){
     t.start()
      Thread.sleep(300)
    }
  }

}
