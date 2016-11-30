package colossus.examples

import akka.actor.{Actor, ActorRef, Props}
import akka.util.ByteString
import colossus.IOSystem
import colossus.core._
import colossus.service.{Codec, DecodedResult}
import colossus.controller._
import java.net.InetSocketAddress
import scala.concurrent.duration._

/*
 * The controller layer adds generalized message processing to a connection.  A
 * Controller provides the ability to decode incoming data in to messages and
 * encode output messages.  There is no coupling between input and output
 * messages.  The service layer extends this layer to add request/response
 * semantics.
 *
 * This example is a simple chat server built on the controller layer.  This example is largely
 * a motivator for cleaning up the controller API.  It should become simpler as
 * we improve the interface.
 */

trait ChatMessage {
  def formatted: String
}
case class Chat(user: String, message: String) extends ChatMessage {
  def formatted = s"$user: $message\r\n"
}
case class Status(message: String) extends ChatMessage {
  def formatted = s"> $message\r\n"
}

class ChatCodec extends Codec[ChatMessage, String]{
  import colossus.parsing.Combinators._

  val parser = stringUntil('\r') <~ byte

  def decode(data: DataBuffer): Option[DecodedResult[String]] = parser.parse(data).map{DecodedResult.Static(_)}

  def encode(message: ChatMessage)= DataBuffer(ByteString(message.formatted))

  def reset(){}

}

class Broadcaster extends Actor {
  import Broadcaster._


  val clients = collection.mutable.Set[ActorRef]()

  def broadcast(message: ChatMessage) {
    clients.foreach{_ ! message}
  }

  def receive = {
    case ClientOpened(user) => {
      clients += sender()
      broadcast(Status(s"$user has joined"))
    }
    case ClientClosed(user) => {
      clients -= sender()
      broadcast(Status(s"$user has left"))
    }
    case m: ChatMessage => broadcast(m)
  }
}

object Broadcaster {
  sealed trait BroadcasterMessage
  case class ClientOpened(user: String) extends BroadcasterMessage
  case class ClientClosed(user: String) extends BroadcasterMessage
}

class ChatHandler(broadcaster: ActorRef, context: ServerContext)
extends Controller[String, ChatMessage](new ChatCodec, ControllerConfig(50, 10.seconds), context.context)
with ProxyActor with ServerConnectionHandler {

  implicit val namespace = context.server.namespace

  sealed trait State
  object State {
    case object LoggingIn extends State
    case class LoggedIn(user: String) extends State
  }
  import State._
  protected var currentState: State = LoggingIn

  // Members declared in colossus.core.ConnectionHandler
  override def connected(endpoint: colossus.core.WriteEndpoint) {
    super.connected(endpoint)
    push(Status("Please enter your name")){_ => ()}
  }

  def receive = {
    case c: ChatMessage => push(c){_ => ()}
  }

  def processMessage(message: String) {
    currentState match {
      case LoggingIn => {
        val user = message.split(" ")(0)
        currentState = LoggedIn(user)
        push(Status("Logged in :)")){_ => ()}
        broadcaster ! Broadcaster.ClientOpened(user)
      }
      case LoggedIn(user) => {
        broadcaster ! Chat(user, message)
      }
    }
  }

  override def connectionTerminated(cause: DisconnectCause) {
    super.connectionTerminated(cause)
    currentState match {
      case LoggingIn => {}
      case LoggedIn(user) => {
        broadcaster ! Broadcaster.ClientClosed(user)
      }
    }
  }

  override def shutdown(){
    push(Status("goodbye")){_ => ()}
    super.shutdown()
  }



}

object ChatExample {

  def start(port: Int)(implicit io: IOSystem): ServerRef = {
    val broadcaster = io.actorSystem.actorOf(Props[Broadcaster])

    Server.basic("chat", port)(context => new ChatHandler(broadcaster, context))

  }

}

