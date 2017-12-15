package colossus.examples

import colossus.IOSystem
import colossus.service.Callback.Implicits._
import colossus.protocols.http._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object BenchmarkExample {

  implicit object JsonBody extends HttpBodyEncoder[HelloWorld] {
    private val mapper: ObjectMapper = new ObjectMapper().registerModule(DefaultScalaModule)
    val contentType: String = ContentType.ApplicationJson
    def encode(helloWorld: HelloWorld): HttpBody = new HttpBody(mapper.writeValueAsBytes(helloWorld))
  }

  case class HelloWorld(message: String)

  def helloWorld: HelloWorld = HelloWorld("Hello, World!")

  def start(port: Int)(implicit io: IOSystem) {
    HttpServer.basic("benchmark", port) {
      case req if req.head.url == "/plaintext" => req.ok("Hello, World!")
      case req if req.head.url == "/json"      => req.ok(helloWorld)
    }
  }
}
