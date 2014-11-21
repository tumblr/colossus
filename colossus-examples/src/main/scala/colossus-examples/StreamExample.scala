package colossus.examples

import colossus.IOSystem
import colossus.core._

import akka.actor._
import akka.util.ByteString
import scala.concurrent.duration._

class MessageProvider extends Actor {
  import MessageProvider._
  import context.dispatcher
  var num = 0
  def receive = {
    case NextMessage => {
      context.system.scheduler.scheduleOnce(500.milliseconds, sender, s"message_$num")
      num += 1
    }
  }
}
object MessageProvider {
  case object NextMessage
}

class StreamDelegator(provider: ActorRef, server: ServerRef, worker: WorkerRef) extends Delegator(server, worker) {
  var id = 0
  def nextId = {id += 1;id}
  implicit val workerRef: ActorRef = worker.worker
  def acceptNewConnection = {
    val handler = server.system.actorSystem.actorOf(Props(classOf[StreamWorker], provider))
    Some(AsyncHandler.serverHandler(handler, worker)(server.system.actorSystem))
  }
}

class StreamWorker(provider: ActorRef) extends Actor with ActorLogging {
  import ConnectionEvent._
  import ConnectionCommand._
  import MessageProvider._

  def receive = {
    case Connected => {
      log.info("New Stream client connected")
      context.become(alive(sender))  
      provider ! NextMessage
    }
  }


  
  def alive(connection: ActorRef): Receive = {
    case message: String => {
      connection ! Write(ByteString(message + "\r\n"), AckLevel.AckAll)
    }
    case WriteAck(WriteStatus.Complete) => provider ! NextMessage
    case WriteAck(WriteStatus.Partial) => context.become(overflow(connection))
    case WriteAck(WriteStatus.Zero) => context.become(overflow(connection))
    case WriteAck(WriteStatus.Failed) => {
      log.error("Failed to write")
      context stop self
    }

    case ConnectionTerminated(cause) => {
      log.info(s"Connection Closed: $cause")
      context stop self
    }
  }

  def overflow(connection: ActorRef): Receive = {
    case ReadyForData => {
      provider ! NextMessage
      context.become(alive(connection))
    }
    case ConnectionTerminated(cause) => {
      context stop self
    }
  }


}

object StreamExample {

  def start(port: Int)(implicit io: IOSystem): ServerRef = {
    //stream example
    val provider = io.actorSystem.actorOf(Props[MessageProvider], name = "stream-provider")
    val streamConfig = ServerConfig(
      name = "stream",
      settings = ServerSettings(
        port = port,
        maxIdleTime = Duration.Inf
      ),
      delegatorFactory = (s, a) => new StreamDelegator(provider, s, a)
    )
    
    Server(streamConfig)
  }


}

