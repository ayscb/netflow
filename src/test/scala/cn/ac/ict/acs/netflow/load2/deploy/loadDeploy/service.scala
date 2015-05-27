package cn.ac.ict.acs.netflow.load2.deploy.loadDeploy

import akka.actor.{ActorRef, Actor, ActorSystem, Props}
import cn.ac.ict.acs.netflow.NetFlowConf
import akka.testkit.{ImplicitSender, DefaultTimeout, TestKit, TestActorRef}
import cn.ac.ict.acs.netflow.util.Utils
import com.typesafe.config.ConfigFactory
import org.scalatest._
import akka.actor._
import akka.testkit._

/**
 * Created by ayscb on 2015/5/21.
 */

object LoadMasterSuite{
  val configure =
    """
      akka {
            loglevel = "WARNING"
        }
    """.stripMargin
}
class LoadMasterSuite( _system : ActorSystem)
  extends TestKit(_system)
  with FunSuiteLike with Matchers with BeforeAndAfterAll
  with DefaultTimeout with PrivateMethodTester with ImplicitSender {

  def this()
  = this(ActorSystem("master",ConfigFactory.parseString(LoadMasterSuite.configure)))

  var masterRef: TestActorRef[LoadMaster] = _
  var master: LoadMaster = _
  var dummyMaster: ActorRef = _
  val conf = new NetFlowConf()

  override def beforeAll(): Unit = {
    masterRef = TestActorRef(
      new LoadMaster(Utils.localHostName(),19898, 19799, conf))
    master = masterRef.underlyingActor
  }

  override def afterAll(): Unit = {
    shutdown()
  }

  test("Register success"){

  }

}

