package colossus
package testkit

import colossus.metrics.MetricSystem
import core._

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

  /**
   * Convenience function for using an IOSystem.  This will create an IOSystem, and ensure its shutdown.
   * @param f
   */
  def withIOSystem(f: IOSystem => Any) {
    val sys = IOSystem("test-system-" + System.currentTimeMillis.toString, Some(2), MetricSystem.deadSystem)
    try {
      f(sys)
    } finally {
      shutdownIOSystem(sys)
    }
  }

  /**
   * Shuts down the IOSystem, and ensures all Servers have been terminated.
   * @param sys
   */
  def shutdownIOSystem(sys : IOSystem) {
    implicit val ec = mySystem.dispatcher
    val probe = TestProbe()
    probe.watch(sys.workerManager)
    val registeredServers = Await.result(sys.registeredServers, 500.milliseconds)
    val watches = registeredServers.map { ref =>
      val p = TestProbe()
      p.watch(ref.server) //implicit map from ServerRef -> ActorRef? mayhaps
      (p, ref.server)
    }
    sys.shutdown()
    probe.expectTerminated(sys.workerManager)
    watches.foreach{case (p, ac) => p.expectTerminated(ac)}
  }

  /**
   * Waits for a Server to be in the specified status.
   * @param server
   * @param waitTime
   * @param serverStatus Defaults to ServerStatus.Bound
   */
  def waitForServer(server: ServerRef, waitTime: FiniteDuration = 500.milliseconds, serverStatus : ServerStatus = ServerStatus.Bound) {
    var attempts = 0
    val MaxAttempts = 20
    val waitPerAttempt = waitTime / MaxAttempts

    while (attempts < MaxAttempts && server.serverState.serverStatus != serverStatus) {
      Thread.sleep(waitPerAttempt.toMillis)
      attempts += 1
    }
    if (attempts == MaxAttempts) {
      throw new Exception("Failed waiting for Server to start")
    }

  }

  /**
   * Convenience function for createing an IOSystem and Server, based on the Delegator factory passed in.  By default the
   * created server will be listening on [[TEST_PORT]], and will have all of the default values listed in [[colossus.core.ServerSettings]]
   * @param factory The factory to create Delegators
   * @param customSettings An custom settings which will override the defaults found in [[colossus.core.ServerSettings]]
   * @param waitTime Amount of time to wait for the Server to be ready
   * @param f The function which runs tests.
   * @return
   */
  def withIOSystemAndServer(factory: Delegator.Factory,
                           customSettings: Option[ServerSettings] = None,
                           waitTime: FiniteDuration = 500.milliseconds)(f : (IOSystem, ServerRef) => Any ) = {
    withIOSystem { implicit io =>

      val config = ServerConfig(
        name = "async-test",
        settings = customSettings.getOrElse(ServerSettings(
          port = TEST_PORT
        )),
        delegatorFactory = factory
      )
      val server = Server(config)
      waitForServer(server, waitTime)
      f(io, server)
      end(server)
    }
  }

  def withServer(handler: ServerContext => ServerConnectionHandler)(op: ServerRef => Any) {
    withIOSystem { implicit io =>
      val server = Server.basic("test-server", TEST_PORT)(handler)
      waitForServer(server)
      op(server)
      end(server)
    }
  }

  /**
   * Shuts down, and asserts that a Server has been terminated.
   * @param server
   */
  def end(server: ServerRef) {
    val probe = TestProbe()
    probe watch server.server
    server.shutdown()
    probe.expectTerminated(server.server)
  }

  /**
   * Convenience function which waits for a Server,and then shuts the Server down after the function runs.
   * @param server
   * @param op
   * @return
   */
  def withServer(server:ServerRef)(op: => Any) {
    waitForServer(server)
    try {
      op
    } finally {
      end(server)
    }
  }
}
