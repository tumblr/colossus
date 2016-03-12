package colossus.examples


import colossus._
import colossus.core.{Initializer, Server, ServerRef, ServerSettings}
import service._
import Callback.Implicits._
import protocols.http._

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

object BenchmarkService {

  implicit object JsonBody extends HttpBodyEncoder[JValue] {
    val jsonHeader            = HttpHeader("Content-Type", "application/json")
    def encode(json: JValue)  = new HttpBody(compact(render(json)).getBytes("UTF-8"), Some(jsonHeader))
  }

  val response          = HttpBody("Hello, World!")
  val serverHeader      = HttpHeader("Server", "Colossus")


  def start(port: Int)(implicit io: IOSystem) {

    val serverConfig = ServerSettings(
      port = port,
      maxConnections = 16384,
      tcpBacklogSize = Some(1024)
    )
    val serviceConfig = ServiceConfig(
      requestMetrics = false
    )

    val server = Server.start("benchmark", serverConfig) { new Initializer(_) {

      val dateHeader = new DateHeader

      def onConnect = ctx => new Service[Http](serviceConfig, ctx){ 
        def handle = { 
          case request if (request.head.url == "/plaintext") => {
            request.ok(response, HttpHeaders(serverHeader, dateHeader))
          } 
          case request if (request.head.url == "/json") => {
            val json: JValue = ("message" -> "Hello, World!")
            request.ok(json, HttpHeaders(serverHeader, dateHeader))
          }
        }
      }
    }}

  }

}


