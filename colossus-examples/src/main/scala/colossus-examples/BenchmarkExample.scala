package colossus.examples


import colossus._
import colossus.core.{Initializer, Server, ServerRef, ServerSettings}
import service._
import Callback.Implicits._
import protocols.http._
import Http.defaults._

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

object BenchmarkService {

  implicit object JsonBody extends HttpBodyEncoder[JValue] {
    val jsonHeader            = HttpHeader("Content-Type", "application/json")
    def encode(json: JValue)  = new HttpBody(compact(render(json)).getBytes("UTF-8"), Some(jsonHeader))
  }

  val json : JValue     = ("message" -> "Hello, World!")
  val plaintext         = HttpBody("Hello, World!")
  val serverHeader      = HttpHeader("Server", "Colossus")

  def start(port: Int)(implicit io: IOSystem) {

    val serverConfig = ServerSettings(
      port = port,
      maxConnections = 16384,
      tcpBacklogSize = Some(1024)
    )

    Server.start("benchmark", serverConfig) { new Initializer(_) {

      val dateHeader = new DateHeader
      val headers = HttpHeaders(serverHeader, dateHeader)

      def onConnect = new Service[Http](_){
        def handle = {
          //case req => req.ok(plaintext, headers)
          case req if (req.head.url == "/plaintext")  => req.ok(plaintext, headers)
          //case req if (req.head.url == "/json")       => req.ok(json, headers)
          case req if (req.head.url == "/echo")       => req.ok(req.toString, headers)
        }
      }

    }}

  }

}





