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
package cn.ac.ict.acs.netflow.query.broker

import scala.concurrent.duration._

import akka.actor._
import akka.actor.SupervisorStrategy.Stop

import org.json4s.DefaultFormats

import spray.http.StatusCode
import spray.http.StatusCodes._
import spray.httpx.Json4sSupport
import spray.routing.RequestContext

import cn.ac.ict.acs.netflow._
import cn.ac.ict.acs.netflow.query.RestMessage

class RequestHandler(
    rc: RequestContext,
    requestMessage: RestMessage,
    master: ActorSelection,
    conf: NetFlowConf
  ) extends Actor with Json4sSupport {

  implicit def json4sFormats = DefaultFormats

  override def preStart(): Unit = {
    master ! requestMessage
  }

  val receiveTimeOut = conf.getLong("netflow.broker.request.timeout", 30).seconds
  context.setReceiveTimeout(receiveTimeOut)

  def receive = {
    case responseMessage: RestMessage => {
      complete(OK, responseMessage)
    }
    case ReceiveTimeout => {
      complete(GatewayTimeout, NetFlowError("Request QueryMaster Timeout"))
    }
  }

  def complete[T <: AnyRef](status: StatusCode, obj: T) = {
    rc.complete(status, obj)
    context.stop(self)
  }

  override val supervisorStrategy = {
    OneForOneStrategy() {
      case e => {
        complete(InternalServerError, NetFlowError(e.getMessage))
        Stop
      }
    }
  }
}
