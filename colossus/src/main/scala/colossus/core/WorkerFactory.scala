package colossus.core

import akka.actor.{ActorContext, ActorRef, Props}
import colossus.IOSystem

private[colossus] trait WorkerFactory {
  def createWorker(id: Int, ioSystem: IOSystem, context: ActorContext): ActorRef
}

private[colossus] object DefaultWorkerFactory extends WorkerFactory {
  override def createWorker(id: Int, ioSystem: IOSystem, context: ActorContext): ActorRef = {
    val workerConfig = WorkerConfig(
      workerId = id,
      io = ioSystem
    )

    val worker = context.actorOf(
      Props(classOf[Worker], workerConfig).withDispatcher("server-dispatcher"),
      name = s"worker-$id"
    )

    context.watch(worker)
    worker
  }
}
