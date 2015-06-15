package cn.ac.ict.acs.netflow.load.utils

import java.util.concurrent.{TimeUnit, ScheduledFuture, Executors}

/**
 * Created by ayscb on 15-6-14.
 */
object T_schude {

  private val sc = Executors.newScheduledThreadPool(2)
  private var lastWriter: ScheduledFuture[_] = null

  private def task = new Runnable {
    override def run(): Unit = {
      println(s"############# start ${System.currentTimeMillis()}")
      Thread.sleep(3000)
      println(s"#############   end ${System.currentTimeMillis()}")
      println()
    }
  }

  private def startSchedTask() : Unit = {
    require(lastWriter == null)
    lastWriter = sc.schedule(task,3,TimeUnit.SECONDS)
  }

  def updateSchedTask(): Boolean = {
    if(lastWriter == null){
      lastWriter = sc.schedule(task,3,TimeUnit.SECONDS)
      return true
    }

    if(lastWriter.isDone){
      return false
    }


    if(lastWriter.cancel(false)){
      lastWriter = sc.schedule(task,3,TimeUnit.SECONDS)
      true
    }else{
      println("canale ........")
      false
    }
  }


  def main(args: Array[String]) {

    var s = 500
    startSchedTask()
    while(true){
      Thread.sleep(s + 500)
      println(updateSchedTask() + " " + s.toString)
      s += 500
    }
  }
}
