package colossus
package testkit

import core._
import metrics._
import service._

import akka.actor._
import akka.testkit.TestProbe
import akka.testkit.CallingThreadDispatcher

import scala.concurrent.ExecutionContext

object FakeIOSystem {
  def apply()(implicit system: ActorSystem): IOSystem = {
    IOSystem(system.deadLetters, IOSystemConfig("FAKE", 0), MetricSystem.deadSystem, system)
  }

  def fakeWorkerRef(implicit system: ActorSystem): (TestProbe, WorkerRef) = {
    val probe = TestProbe()
    implicit val aref = probe.ref
    val ref = WorkerRef(0, new LocalCollection, probe.ref, apply())
    (probe, ref)
  }

  def withManagerProbe()(implicit system: ActorSystem): (IOSystem, TestProbe) = {
    val probe = TestProbe()
    val sys = IOSystem(probe.ref, IOSystemConfig("FAKE", 0), MetricSystem.deadSystem, system)
    (sys, probe)
  }

  /**
   * Returns an entirely in-thread CallbackExecutor
   */
  def callingThreadCallbackExecutor(implicit system: ActorSystem): CallbackExecutor = {
    class FakeExecutor extends Actor with CallbackExecution {
      def receive = handleCallback
    }
    val ref = system.actorOf(Props[FakeExecutor].withDispatcher(CallingThreadDispatcher.Id))
    val executor = new ExecutionContext {
      def reportFailure(t: Throwable) { t.printStackTrace() }
      def execute(runnable: Runnable) {runnable.run()}
    }
    CallbackExecutor(executor, ref)
  }
}

