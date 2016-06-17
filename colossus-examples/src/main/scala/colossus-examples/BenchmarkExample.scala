package colossus.examples


import colossus._
import colossus.core.{Server, ServerRef, ServerSettings}
import service._
import Callback.Implicits._
import protocols.http._
import Http.defaults._

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

object BenchmarkService {

  implicit object JsonBody extends HttpBodyEncoder[JValue] {
    def encode(json: JValue)  = {
      new HttpBody(compact(render(json)).getBytes("UTF-8")).withContentType("application/json")
    }
  }

  val json : JValue     = ("message" -> "Hello, World!")
  val plaintext         = HttpBody("Hello, World!")

  def start(port: Int)(implicit io: IOSystem) {

    HttpServer.basic("benchmark", port) { 
      //case req => req.ok(plaintext)
      case req if (req.head.url == "/plaintext")  => req.ok(plaintext)
      case req if (req.head.url == "/json")       => req.ok(json)
    }

  }

}





