package cn.as.ict.acs.netflow.load.master

import cn.ac.ict.acs.netflow.load.Rule

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Created by ayscb on 6/24/15.
 */
object Test_loadmaster {

  private val destAddr = mutable.LinkedHashSet.empty[String]
  private val destAddr2idx = mutable.HashMap.empty[String, Int]
  private val srcMapping = mutable.HashMap.empty[String, Int]

  private def notifyRulesToAllWorkers(rules: ArrayBuffer[Rule]): Unit = {
    rules.foreach(f = rule => {
      if (destAddr.contains(rule.dest)) {
        val idx: Int = destAddr2idx(rule.dest)
        srcMapping(rule.src) = idx
      } else {
        val idx = destAddr.size
        destAddr2idx(rule.dest) = idx
        srcMapping(rule.src) = idx
        destAddr += rule.dest
      }
    })

    val key = srcMapping.iterator.map(x => x._1 + ":" + x._2).mkString(";")
    val value = destAddr.iterator.mkString(";")
    println(key)
    println(value)
  }

  private def pushRuleToWorker(): Unit = {
    val key = srcMapping.iterator.map(x => x._1 + ":" + x._2).mkString(";")
    val value = destAddr.iterator.mkString(";")
    println(key)
    println(value)
  }

  def main(args: Array[String]) {
    val rules = new ArrayBuffer[Rule]()
    rules += Rule("asd","val1")
    rules += Rule("asdf","val1")
    rules += Rule("asdg","val2")
    rules += Rule("asdh","val1")
    rules += Rule("asdj","val3")
    rules += Rule("asdk","val2")
    rules += Rule("asdl","val4")
    notifyRulesToAllWorkers(rules)
    pushRuleToWorker()
  }
}
