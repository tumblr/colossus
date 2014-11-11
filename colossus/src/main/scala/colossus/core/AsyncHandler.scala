package colossus
package core

import akka.actor._
import akka.util.ByteString

/**
 * A Watched handler allows an actor to be tied to a connection.  The worker
 * will watch the actor, and on termination will shutdown the connection
 * associated with the actor
 */
trait WatchedHandler extends ConnectionHandler {
  def watchedActor: ActorRef
}

//the sender (the worker
case class AsyncHandler(handler: ActorRef, worker: WorkerRef) extends WatchedHandler with ClientConnectionHandler {
  import ConnectionCommand._
  import ConnectionEvent._

  implicit val sender = worker.worker

  private var endpointOpt: Option[WriteEndpoint] = None
  def endpoint = endpointOpt.getOrElse(throw new Exception("Attempted to use non-connected endpoint"))
  def watchedActor = handler
  def receivedData(data: DataBuffer) {
    handler ! ReceivedData(ByteString(data.takeAll))
  
  }

  def bound(id: Long, worker: WorkerRef){}


  def connected(e: WriteEndpoint) {
    endpointOpt = Some(e)
    handler ! Connected(endpoint.id)
  }
  def readyForData() {
    handler ! ReadyForData
  }

  override protected def connectionClosed(cause : DisconnectCause) {
    handler ! ConnectionTerminated(cause)
    endpointOpt = None
  }
  override protected def connectionLost(cause : DisconnectError) {
    handler ! ConnectionTerminated(cause)
    endpointOpt = None
  }

  //definitely some weirdness here
  def receivedMessage(message: Any, sender: ActorRef) {
    import AckLevel._
    import WriteStatus._
    if (sender == handler) {
      message match {
        case Write(data, ackLevel) => {
          val (send, status) = endpointOpt.map{e => 
            val status = e.write(DataBuffer(data))
            val send = if ((status == Complete || status == Partial) && ackLevel == AckSuccess) {
              true
            } else if ((status == Failed || status == Zero) && ackLevel == AckFailure) {
              true
            } else if (ackLevel == AckAll) {
              true
            } else {
              false
            }
            (send, status)
          }.getOrElse{ (ackLevel == AckAll || ackLevel == AckFailure, Failed)}
          if (send) {
            handler ! WriteAck(status)
          }        
        }
        case Disconnect => endpointOpt.foreach{_.disconnect()}
      }
    } else {
      handler.!(message, sender)
    }
  }
  def connectionFailed() {
    handler ! ConnectionFailed
  }

}

object AsyncHandler {

  /**
   * Wires together an ActorHandler with an AsyncHandler and registers your
   * handler with it.  Basically the AsyncHandler sends the raw messages to the
   * ActorHandler, which then forwards them to your handler.
   */
  def serverHandler(handler: ActorRef, worker: WorkerRef)(implicit fact: ActorRefFactory): AsyncHandler = {
    val actor = fact.actorOf(Props[ActorHandler])
    actor ! ActorHandler.RegisterListener(handler)
    AsyncHandler(actor, worker)
  }
    
}

/**
 *
 * These events are sent from the underlying async handler to the handler actor and also forwarded to the listener
 *
 * Notice for all of these events, the sender is always going to be the worker
 * to which the connection is bound.
 */
sealed trait ConnectionEvent

/**
 *
 * These events are sent from the underlying async handler to the handler actor and also forwarded to the listener
 *
 * Notice for all of these events, the sender is always going to be the worker
 * to which the connection is bound.
 */
sealed trait ClientConnectionEvent extends ConnectionEvent
object ConnectionEvent {
  case class ReceivedData(data: ByteString) extends ConnectionEvent
  case object ReadyForData extends ConnectionEvent
  //maybe include an id or something
  case class WriteAck(status: WriteStatus) extends ConnectionEvent
  case class ConnectionTerminated(cause : DisconnectCause) extends ConnectionEvent
  case class Connected(id: Long) extends ConnectionEvent
  //for client connections
  case object ConnectionFailed extends ClientConnectionEvent
}

/**
 * These messages are sent to async connection actors to interact with connections
 */
sealed trait ConnectionCommand
object ConnectionCommand {
  case class Write(data: ByteString, ackLevel: AckLevel) extends ConnectionCommand
  //notice that this object is different from WorkerCommand.Disconnect since
  //this one doesn't include the connection id (which is intended to be hidden
  //from the listener)
  case object Disconnect extends ConnectionCommand
}

/**
 * AckLevel is used in Asynchronous write messages to determine how to react to
 * writes.  This is useful when interacting with a connection from an actor and
 * needing to handle backpressure when writing large amounts of data.  
 *
 * If AckFailure is selected, WriteStatus's of "Partial" (data partially
 * written but buffered, next write will fail), "Zero" (no data was written,
 * buffer full), and "Failed" (error, probably disconnected) will generate
 * WriteAck messages.
 *
 * If AckSuccess is selected, "Partial" and "Success" (data fully written) will
 * generate WriteAck
 *
 * The simplest approach is to AckAll and only send ByteStrings one at a time,
 * waiting for the WriteAck message each time.  The highest-throughput approach
 * is  to use AckFailure, and send messages as fast as possible, but be aware that
 * you will be entirely responsible for resending failed messages.
 *
 * WriteAck Messages are only sent to the ConnectionHandler's watcher (if one
 * exists, see the WatchedHandler trait), which may NOT necessarily be the
 * sender of the message.
 *
 * If you receive a Partial status, you should not send any more data until receiving a ReadyForData message
 */
sealed trait AckLevel
object AckLevel {
  case object AckSuccess  extends AckLevel
  case object AckFailure  extends AckLevel
  case object AckAll      extends AckLevel
  case object AckNone     extends AckLevel
}


