package colossus.task

import akka.actor._
import colossus.IOSystem
import colossus.core.{Context, ProxyActor, WorkerItem}

/**
  * A Task is basically a way to run an arbitrary function inside a worker.
  * Tasks can open connections and interact with actors through a built-in proxy
  * actor.
  *
  */
abstract class Task(val context: Context) extends WorkerItem with ProxyActor {

  override def onBind() {
    super.onBind()
    run()
  }

  def run()

}

object Task {

  def start(creator: Context => Task)(implicit io: IOSystem): ActorRef = {
    io.bindWithProxy(creator)
  }

}
