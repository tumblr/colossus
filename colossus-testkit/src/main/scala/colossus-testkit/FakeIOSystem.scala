package colossus
package testkit

import core._
import metrics._
import service._

import akka.agent.Agent
import akka.actor._
import akka.testkit.TestProbe

import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._

case class FakeWorker(probe: TestProbe, worker: WorkerRef)

object FakeIOSystem {
  def apply()(implicit system: ActorSystem): IOSystem = {
    new IOSystem("FAKE", 0, MetricSystem.deadSystem, system, (x,y) => system.deadLetters)
  }

  /**
   * Returns a WorkerRef with a TestProbe as the underlying actor.  Returns the probe with the WorkerRef.
   *
   * Important: This WorkerRef's callbackExecutor does NOT work, use `fakeExecutorWorkerRef` instead
   */
  def fakeWorkerRef(implicit system: ActorSystem): (TestProbe, WorkerRef) = {
    val probe = TestProbe()
    implicit val aref = probe.ref
    val ref = WorkerRef(0, probe.ref, apply())
    (probe, ref)
  }
  //use this for new tests
  def fakeWorker(implicit system: ActorSystem) = {
    val (p, w) = fakeWorkerRef
    FakeWorker(p, w)
  }

  /**
   * Returns a ServerRef representing a server in the Bound state
   */
  def fakeServerRef(implicit system: ActorSystem): (TestProbe, ServerRef) = {
    import system.dispatcher
    val probe = TestProbe()
    val config = ServerConfig(
      "/foo",
      (s,w) => ???,
      ServerSettings(987)
    )
    val ref = ServerRef(config, probe.ref, apply(), Agent(ServerState(ConnectionVolumeState.Normal, ServerStatus.Bound)))
    (probe, ref)
  }

  /**
   * Returns a WorkerRef that is able to properly execute callbacks
   */
  def fakeExecutorWorkerRef(implicit system: ActorSystem): WorkerRef = {
    val ex = testExecutor
    WorkerRef(0, testExecutor.executor, FakeIOSystem())
  }

  def withManagerProbe()(implicit system: ActorSystem): (IOSystem, TestProbe) = {
    val probe = TestProbe()
    val sys = new IOSystem("FAKE", 0, MetricSystem.deadSystem, system, (x,y) => probe.ref)
    (sys, probe)
  }

  //prevents accidentally spinning up a new executor per test
  private val exCache: collection.mutable.Map[ActorSystem, CallbackExecutor] = collection.mutable.Map()

  def testExecutor(implicit system: ActorSystem): CallbackExecutor = {
    exCache.get(system).getOrElse {
      val ref = system.actorOf(Props[GenericExecutor].withDispatcher("server-dispatcher"))
      val ex = CallbackExecutor(system.dispatcher, ref)
      exCache(system) = ex
      ex
    }
  }
}


trait GenericCallback {
  def execute()
}

case class CallbackMessage[T](cb: Callback[T]) extends GenericCallback {
  def execute() {
    cb.execute()
  }
}

class GenericExecutor extends Actor with CallbackExecution {
  def receive = handleCallback orElse {
    case g: GenericCallback => {
      g.execute()
    }
  }
}

object CallbackAwait {

  /**
   * Await the result of a Callback.  This *must* be used whenever Callback.fromFuture is used.  Going forward this may be the only way to
   * extract the value from a Callback
   *
   * The Callback is properly executed inside an Actor running in a PinnedDispatcher.  The calling thread blocks until the Callback finishes
   * execution or the timeout is reached
   */
  def result[T](cb: Callback[T], in: Duration)(implicit ex: CallbackExecutor): T = {
    val p = Promise[T]()
    ex.executor ! CallbackMessage(cb.mapTry{t => p.complete(t);t})
    Await.result(p.future, in)
  }

}
