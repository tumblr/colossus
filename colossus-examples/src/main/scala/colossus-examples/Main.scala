package colossus.examples

import colossus._
import akka.actor._
import java.net.InetSocketAddress



object Main extends App {

  println(""" _______  _______  _        _______  _______  _______           _______ """)
  println("""(  ____ \(  ___  )( \      (  ___  )(  ____ \(  ____ \|\     /|(  ____ \""")
  println("""| (    \/| (   ) || (      | (   ) || (    \/| (    \/| )   ( || (    \/""")
  println("""| |      | |   | || |      | |   | || (_____ | (_____ | |   | || (_____ """)
  println("""| |      | |   | || |      | |   | |(_____  )(_____  )| |   | |(_____  )""")
  println("""| |      | |   | || |      | |   | |      ) |      ) || |   | |      ) |""")
  println("""| (____/\| (___) || (____/\| (___) |/\____) |/\____) || (___) |/\____) |""")
  println("""(_______/(_______)(_______/(_______)\_______)\_______)(_______)\_______)""")

  implicit val actorSystem = ActorSystem("COLOSSUS")

  implicit val ioSystem = IOSystem()//"examples", numWorkers = Some(1))

  //the simplest example, an echo server over telnet
  //val telnetServer = TelnetExample.start(9000)

  //http service which communicates with a key/value store over the redis protocol
  val httpServer = HttpExample.start(9001, new InetSocketAddress("localhost", 9002))

  //and here's the key/value store itself
  val keyvalServer = KeyValExample.start(9002)

  //an echo server built only on the core layer
  //val echoServer = EchoExample.start(9003)

  //chat server using the controller layer
  //val chatServer = ChatExample.start(9005)
import java.util.Date
import java.text.SimpleDateFormat

import colossus._
import colossus.core.ServerRef
import service._
import protocols.http._

import net.liftweb.json._
import JsonDSL._
import akka.actor._
import akka.util.ByteString
import scala.concurrent.duration._

class Timestamp(server: ServerRef) extends Actor {
      
  val sdf = new SimpleDateFormat("EEE, MMM d yyyy HH:MM:ss z")
  case object Tick
  import context.dispatcher

  override def preStart() {
    self ! Tick
  }

  def receive = {
    case Tick => {
      server.delegatorBroadcast(sdf.format(new Date()))
      context.system.scheduler.scheduleOnce(1.second, self, Tick)
    }
  }
}
  val response          = "Hello, World!"
  val plaintextHeader   = ("Content-Type", "text/plain")
  val jsonHeader        = ("Content-Type", "application/json")
  val serverHeader      = ("Server", "Colossus")

  val server = Service.serve[Http]("sample", 9007) { context =>

    var dateHeader = ("Date", "???")

    context.receive {
      case ts: String => dateHeader = ("Date", ts)
    }
    
    context.handle { connection =>
      connection.become{ case request =>
        if (request.head.url == "/plaintext") {
          val res = HttpResponse(
            version  = HttpVersion.`1.1`,
            code    = HttpCodes.OK,
            data    = response,
            headers = Vector(plaintextHeader, serverHeader, dateHeader)
          )
          Callback.successful(res)
        } else if (request.head.url == "/json") {
          val json = ("message" -> "Hello, World!")
          val res = HttpResponse(
            version  = HttpVersion.`1.1`,
            code    = HttpCodes.OK,
            data    = compact(render(json)),
            headers = Vector(jsonHeader, serverHeader, dateHeader)
          )
          Callback.successful(res)
        } else {
          Callback.successful(request.notFound("invalid path"))
        }
      }
    }
  }

  val timestamp = ioSystem.actorSystem.actorOf(Props(classOf[Timestamp], server))

}
