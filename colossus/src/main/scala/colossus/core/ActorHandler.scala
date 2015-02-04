package colossus
package core

import akka.actor._
import java.net.InetSocketAddress

/**
 * A Basic actor-based handler that can be used with the AsyncHandler.  A
 * listener actor must register with this actor, after which is can send and
 * receive data across the connection
 *
 * This actor can handle both server and client connections, since it does not
 * handle the initial setup of the connection
 */
class ActorHandler extends Actor with Stash {
  import ConnectionCommand._
  import WorkerCommand._
  import ActorHandler._
  import ConnectionEvent._

  def receive = {
    case RegisterListener(listener) => {
      context.become(binding(listener))
      unstashAll() //this unstash must be here in case we get Bound first
    }
    case other => {
      stash()
    }
  }

  def binding(listener: ActorRef): Receive = {
    case Bound(id) => {
      context.become(waitingForConnected(listener, id))
    }
    case other => {
      stash()
    }
  }


  def waitingForConnected(listener: ActorRef, connectionId: Long): Receive = {
    case Connected => {
      listener ! Connected
      context.become(active(listener, sender(), connectionId))
      unstashAll()
    }
    case ConnectionFailed => {
      listener ! ConnectionFailed
      context stop self
    }
    case other => {
      stash()
    }
  }

  def active(listener: ActorRef, worker: ActorRef, connectionId: Long): Receive = {
    case c: ConnectionEvent => c match {
      case c: ConnectionTerminated => {
        listener ! c
        context stop self
      }
      case ConnectionFailed => {
        listener ! ConnectionFailed
        context stop self
      }
      case other => listener ! other
    }
    case w: Write => {
      worker ! Message(connectionId, w)
    }
  }

}

object ActorHandler {

  def apply(address: InetSocketAddress, name: String = "async-client")(implicit io: IOSystem) = {
    val actor = io.actorSystem.actorOf(Props(classOf[ActorHandler]), name = name)
    io ! IOCommand.BindAndConnectWorkerItem(address, _ => new AsyncHandler(actor))
    actor
  }

  case class RegisterListener(listener: ActorRef)

}
