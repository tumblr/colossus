package colossus.core

import akka.actor.{ActorContext, ActorRef, Props}
import akka.agent.Agent
import akka.testkit.TestProbe
import colossus.IOSystem
import colossus.testkit.ColossusSpec
import org.scalatest.concurrent.Eventually

import scala.collection.immutable.IndexedSeq
import scala.concurrent.duration._

class WorkerManagerSpec extends ColossusSpec with Eventually{

  "WorkerManager" must {

    "send an idle only after all workers ack the previous idle check" in {
      withIOSystem{ io =>
        //we need to create a separate manager.
        implicit val ec = system.dispatcher
        //need to use the same number of ioSystem workers, since WorkerManager uses this to control the # of workers created
        val probes: IndexedSeq[TestProbe] = (1 to io.numWorkers).map(_ => TestProbe())
        val probesIter = probes.iterator
        var workerRefs : Seq[WorkerRef] = Seq()

        //create a "Worker" which is just a probe.
        val workerFact = new WorkerFactory {
          override def createWorker(id: Int, ioSystem: IOSystem, context: ActorContext): ActorRef = {
            val ref = probesIter.next().ref
            val worker = WorkerRef(id, ref, ioSystem)
            workerRefs = workerRefs :+ worker
            ref
          }
        }

        val agent = Agent(Seq[WorkerRef]())
        val manager: ActorRef = system.actorOf(Props(classOf[WorkerManager], agent, io, workerFact), name = s"idle-check-manager")

        //give the WorkerManager some time to create the Workers
        eventually{
          workerRefs must have size io.numWorkers
        }

        //lets now report Workers as ready, so that the WorkerManager changes its state
        workerRefs.foreach{x =>
          manager.tell(WorkerManager.WorkerReady(x), x.worker)
        }

        //this allows for a bit of timing leeway
        val window = WorkerManager.IdleCheckFrequency + 50.milliseconds

        //manger should be sending check messages
        probes.foreach{_.expectMsg(window, Worker.CheckIdleConnections)}

        //no more messages should be sent, until acks are received
        probes.foreach{_.expectNoMsg(window * 2)}

        //lets now send the acks back
        probes.foreach{x => manager.tell(WorkerManager.IdleCheckExecuted, x.ref)}

        //now, we should be getting checks again
        probes.foreach{_.expectMsg(window, Worker.CheckIdleConnections)}
      }
    }
  }
}
