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
package cn.ac.ict.acs.netflow.deploy.broker

import akka.actor.{ActorSelection, ActorContext, Props, Actor}

import spray.http.MediaTypes._
import spray.routing.{RequestContext, Route, HttpService}

import cn.ac.ict.acs.netflow.NetFlowError
import cn.ac.ict.acs.netflow.deploy.RestMessage
import cn.ac.ict.acs.netflow.deploy.RestMessages._

class FakeMaster extends Actor {
  def receive = {
    case RestRequestQueryMasterStatus => sender ! XResponse("RestRequestQueryMasterStatus")
    case RestRequestAllJobsInfo => sender ! XResponse("RestRequestAllJobsInfo")
    case RestRequestJobInfo(jobId)=> sender ! XResponse(s"RestRequestJobInfo: $jobId")
    case RestRequestSubmitJob(jsonStr) => sender ! XResponse(s"RestRequestSubmitJob: $jsonStr")
    case RestRequestKillJob(jobId) => sender ! XResponse(s"RestRequestKillJob: $jobId")
  }
}

class RestBroker extends Actor with RestService with BrokerLike {

  implicit def actorRefFactory: ActorContext = context

  val masterActor = context.actorOf(Props[FakeMaster], "fakeMaster")

  var master: ActorSelection =
    context.actorSelection("akka://netflowRest/user/RestBroker/fakeMaster")

  def receive = lifecycleMaintenance orElse runRoute(routeImpl)
}

trait BrokerLike {
  this: RestBroker =>

  val lifecycleMaintenance: Receive = {
    case NetFlowError(msg) => println(msg)
  }
}

trait RestService extends HttpService {
  this: RestBroker =>

  val routeImpl = respondWithMediaType(`application/json`) {
    path("status") {
      get {
        requestQueryMaster {
          RestRequestQueryMasterStatus
        }
      }
    } ~
    pathPrefix("netflow" / "v1" / "jobs") {
      pathEndOrSingleSlash {
        get {
          requestQueryMaster {
            RestRequestAllJobsInfo
          }
        } ~
        post {
          entity(as[String]) { jobBody =>
            requestQueryMaster {
              RestRequestSubmitJob(jobBody)
            }
          }
        }
      } ~
      path(Rest) { jobId =>
        get {
          requestQueryMaster {
            RestRequestJobInfo(jobId)
          }
        } ~
        delete {
          requestQueryMaster {
            RestRequestKillJob(jobId)
          }
        }
      }
    }
  }

  def requestQueryMaster(message: RestMessage): Route = {
    ctx => handleRequest(ctx, message, master)
  }

  def handleRequest(rc: RequestContext, message: RestMessage, master: ActorSelection) = {
    context.actorOf(Props(classOf[RequestHandlerActor], rc, message, master))
  }
}
