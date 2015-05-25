package cn.ac.ict.acs.netflow.broker

import akka.actor._
import akka.testkit._
import cn.ac.ict.acs.netflow.DeployMessages._
import cn.ac.ict.acs.netflow.{Logging, NetFlowConf}
import com.typesafe.config.ConfigFactory

import org.scalatest._

class BrokerSuite(_system: ActorSystem)
  extends TestKit(_system)
  with FunSuiteLike with Matchers with BeforeAndAfterAll
  with DefaultTimeout with PrivateMethodTester with ImplicitSender {

  def this() = this(ActorSystem("BrokerTest",
    ConfigFactory.parseString(BrokerSuite.config)))

  var broker: TestActorRef[RestBroker] = _
  var bareBroker: RestBroker = _
  var dummyMaster: ActorRef = _

  override def beforeAll(): Unit = {
    broker = TestActorRef(
      new RestBroker("fake",19898, 19799,
        Array("netflow-query://fake:7977"),
        new NetFlowConf(false)))
    bareBroker = broker.underlyingActor
    dummyMaster = _system.actorOf(Props(new DummyMaster), "dummy")
  }

  override def afterAll(): Unit = {
    shutdown()
  }

  test("Register success") {
    bareBroker.registered should be (false)
    bareBroker.connected should be (false)
    bareBroker.master should be (null)
    broker ! RegisteredBroker("netflow-query://fake:7977", "http://fake:19991")
    bareBroker.registered should be (true)
    bareBroker.connected should be (true)
    bareBroker.master should not be (null)
  }

  test("Send heartbeat") {
    broker ! RegisteredBroker("netflow-query://fake:7977", "http://fake:19991")
    bareBroker.master = _system.actorSelection(self.path)
    broker ! SendHeartbeat
    expectMsgType[Heartbeat]
  }

  test("Master changed") {
    broker ! MasterChanged("netflow-query://fake:7977", "http://fake:19991")
    expectMsgType[BrokerStateResponse]
  }
}

object BrokerSuite {
  val config = """
    akka {
      loglevel = "WARNING"
    } """
}

class DummyMaster extends Actor with Logging {
  def receive = {
    case Heartbeat(id) =>
      logDebug(s"heartBeat from $id")
  }
}