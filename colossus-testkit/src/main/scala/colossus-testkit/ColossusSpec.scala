package colossus
package testkit

import core._

import akka.pattern.ask

import org.scalatest._

import akka.actor._
import akka.event.Logging
import akka.testkit._
import akka.testkit.TestProbe

import scala.concurrent.Await
import scala.concurrent.duration._
import akka.util.Timeout


abstract class ColossusSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with MustMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("Spec"))

  val TEST_PORT = 19264
  implicit val timeout = Timeout(500.milliseconds)

  implicit val mySystem = system

  val log = Logging(system, "Spec")

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  def withIOSystem(f: IOSystem => Any) {
    val sys = IOSystem("test-system", 2)
    val probe = TestProbe()
    probe.watch(sys.workerManager)
    try {
      f(sys)
    } finally {
      sys.shutdown()
      probe.expectTerminated(sys.workerManager)
    }
  }

  //this is gone next commit!
  def withIOSystemSafe(f : IOSystem => Seq[ActorRef]) {
    val sys = IOSystem("test-system", 2)
    val probe = TestProbe()
    probe.watch(sys.workerManager)
    var actorsToWatch = Seq[(TestProbe, ActorRef)]()
    try {
      val actors = f(sys)
      actorsToWatch = actors.map{a =>
        val p = TestProbe()
        p.watch(a)
        (p, a)
      }
    } finally {
      sys.shutdown()
      probe.expectTerminated(sys.workerManager)
      actorsToWatch.foreach{case (p, ac) => p.expectTerminated(ac)}
    }
  }

  def waitForServer(server: ServerRef, waitTime: FiniteDuration = 500.milliseconds, serverStatus : ServerStatus = ServerStatus.Bound) {
    var attempts = 0
    val MaxAttempts = 20
    val waitPerAttempt = waitTime / MaxAttempts

    while (attempts < MaxAttempts && Await.result(server.server ? Server.GetStatus, waitTime) != serverStatus) {
      Thread.sleep(waitPerAttempt.toMillis)
      attempts += 1
    }
    if (attempts == MaxAttempts) {
      throw new Exception("Failed waiting for Server to start")
    }

  }


  def createServer(factory: Delegator.Factory, customSettings: Option[ServerSettings] = None, waitTime: FiniteDuration = 500.milliseconds): ServerRef = {
    implicit val io = IOSystem("async-test", 2)
    val config = ServerConfig(
      name = "async-test",
      settings = customSettings.getOrElse(ServerSettings(
        port = TEST_PORT
      )),
      delegatorFactory = factory
    )
    val server = Server(config)
    waitForServer(server, waitTime)
    server
  }

  def end(server: ServerRef) {
    val probe = TestProbe()
    probe watch server.server
    server.system.shutdown()
    probe.expectTerminated(server.server)
  }

  def withServer(server:ServerRef)(op: => Any) {
    waitForServer(server)
    try {
      op
    } finally {      
      end(server)
    }
  }
}
