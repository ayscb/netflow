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
package cn.ac.ict.acs.netflow

import scala.concurrent.Future

import akka.actor._

import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.sql.SQLContext

import cn.ac.ict.acs.netflow.util._

class JobActor(
  masterUrl: String,
  jobId: String)
    extends Actor with ActorLogReceive with Logging {
  import JobMessages._
  import context.dispatcher

  var master: ActorSelection = _
  var resultTracker: ActorRef = _

  override def preStart() = {
    logInfo(s"Job $jobId started with master: $masterUrl to be connected")
    val masterAddress = AkkaUtils.toQMAkkaUrl(masterUrl, AkkaUtils.protocol())
    master = context.actorSelection(masterAddress)
    master ! JobLaunched(jobId)
  }

  def receiveWithLogging = {
    case JobInitialize(tpe, query, outputPath, resultCache, sparkMaster) => {
      logInfo(s"Get JobInfo from QueryMaster $query")
      if (resultCache.isDefined) {
        resultTracker = resultCache.get
      }

      val jobFuture = Future {
        runJob(sparkMaster, query, resultTracker, outputPath)
      }

      jobFuture onSuccess {
        case (success, expOpt) => {
          if (success) {
            logInfo(s"Job's main part finished successfully")
            master ! JobFinished(jobId)
          } else {
            logInfo(s"Job failed with exception", expOpt.get)
            master ! JobFailed(jobId, expOpt.get)
          }
        }
      }
    }

    case JobEndACK => {
      logInfo(s"Ok to be shutdown")
      context.system.shutdown()
    }

    case JobNotFound => {
      logWarning(s"No such job exists in QueryMaster, this should never happen")
      context.system.shutdown()
    }
  }

  def runJob(sparkMaster: String, query: Query,
    resultTracker: ActorRef, outputPath: String): (Boolean, Option[Throwable]) = {

    var sc: SparkContext = null

    try {
      val conf = new SparkConf().setMaster(sparkMaster)
      sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)

      // val result = sqlContext.sql(query.sql)

      import sqlContext.implicits._
      val a = sc.parallelize(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11), 5)
      val result = a.toDF()

      if (resultTracker != null) {
        val schema = result.toString
        val sample = result.head(10).map(_.toString)
        val ouputCount = result.count()
        resultTracker ! JobResult(jobId, ResultDetail(schema, sample, ouputCount))
      }
      result.write.json(outputPath + "/" + jobId)

      (true, None)
    } catch {
      case e: Throwable =>
        logError(s"Exception occurred: ${e.getMessage}")
        (false, Some(e))
    } finally {
      if (sc != null) {
        sc.stop()
      }
    }
  }
}

object JobActor {

  // args netflow-query:// jobId
  def main(args: Array[String]) {
    val host = Utils.localHostName()
    val (actorSystem, _) =
      AkkaUtils.createActorSystem("jobOnSpark", host, 0, new NetFlowConf(false))
    actorSystem.actorOf(Props(classOf[JobActor], args(0), args(1)))
    actorSystem.awaitTermination()
  }
}
