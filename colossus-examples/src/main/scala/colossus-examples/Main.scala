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

  implicit val ioSystem = IOSystem("examples", numWorkers = Some(4))

  //the simplest example, an echo server over telnet
  val telnetServer = TelnetExample.start(9000)

  //http service which communicates with a key/value store over the redis protocol
  val httpServer = HttpExample.start(9001, new InetSocketAddress("localhost", 9002))

  //and here's the key/value store itself
  val keyvalServer = KeyValExample.start(9002)

  //an echo server built only on the core layer
  val echoServer = EchoExample.start(9003)

  //chat server using the controller layer
  val chatServer = ChatExample.start(9005)


  import core.DataBuffer
  import service._
  import protocols.http._
  import HttpMethod._
  import UrlParsing._
  import controller._
  import akka.util.ByteString
  import Callback.Implicits._

  Service.serve[StreamingHttp]("stream-proxy", 9090){ context =>
    context.handle{ connection =>
      connection.become {
        case req @ Get on Root => {
          val client = context.clientFor[StreamingHttp]("localhost", 9001)
          client.send(HttpRequest.get("/")).map{response =>
            client.gracefulDisconnect()
            response
          }
        }
        case req @ Get on Root / "x" / Integer(num) => {
          class NumberGenerator extends Generator[DataBuffer] {
            private var current = 0
            def generate(): Option[DataBuffer] = {
              current += 1
              if (current == num) None else Some(DataBuffer(ByteString(current.toString)))
            }
          }
          val pipe = new ChunkEncodingPipe
          pipe.feed(new NumberGenerator, true)
          StreamingHttpResponse(
            HttpResponseHead(HttpVersion.`1.1`, HttpCodes.OK, Vector("Transfer-Encoding" -> "chunked")),
            Some(pipe)
          )
        }
      }
    }
  }


}
