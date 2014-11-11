package colossus
package testkit

import core._
import metrics._

import akka.actor._
import akka.testkit.TestProbe


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
}

