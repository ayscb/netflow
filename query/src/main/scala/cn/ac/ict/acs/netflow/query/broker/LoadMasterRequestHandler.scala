package cn.ac.ict.acs.netflow.query.broker

import scala.concurrent.duration._

import akka.actor.SupervisorStrategy.Stop
import akka.actor.{OneForOneStrategy, ReceiveTimeout, Actor, ActorSelection}

import org.json4s.DefaultFormats

import spray.http.StatusCode
import spray.http.StatusCodes._
import spray.httpx.Json4sSupport
import spray.routing.RequestContext

import cn.ac.ict.acs.netflow._
import cn.ac.ict.acs.netflow.util.ActorLogReceive

class LoadMasterRequestHandler(rc: RequestContext,
  requestMessage: ConfigurationMessage,
  master: ActorSelection,
  conf: NetFlowConf)
  extends Actor with ActorLogReceive with Json4sSupport with Logging {

  implicit def json4sFormats = DefaultFormats

  override def preStart(): Unit = {
    master ! requestMessage
  }

  val receiveTimeOut = conf.getLong("netflow.broker.request.timeout", 30).seconds
  context.setReceiveTimeout(receiveTimeOut)

  def receiveWithLogging = {
    case responseMessage: ConfigurationMessage => {
      complete(OK, responseMessage)
    }
    case ReceiveTimeout => {
      complete(GatewayTimeout, NetFlowError("Request LoadMaster Timeout"))
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

