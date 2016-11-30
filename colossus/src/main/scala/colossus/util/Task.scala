package colossus
package task

import core._

import akka.actor._

/**
 * A Task is basically a way to run an arbitrary function inside a worker.
 * Tasks can open connections and interact with actors through a built-in proxy
 * actor.
 *
 */

abstract class Task(context: Context) extends WorkerItem(context) with ProxyActor {

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

