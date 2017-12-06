package colossus.examples

import colossus.IOSystem
import colossus.service.Callback.Implicits._
import colossus.protocols.http._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object BenchmarkService {

  private val mapper: ObjectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  implicit object JsonEncoder extends HttpBodyEncoder[Array[Byte]] {
    val contentType = Some(ContentType.ApplicationJson)
    def encode(json: Array[Byte]): HttpBody = new HttpBody(json)
  }

  def json: Array[Byte] = mapper.writeValueAsBytes(Map("message" -> "Hello, World!"))
  val plaintext    = HttpBody("Hello, World!")

  def start(port: Int)(implicit io: IOSystem) {
    HttpServer.basic("benchmark", port) {
      case req if req.head.url == "/plaintext" => req.ok(plaintext)
      case req if req.head.url == "/json"      => req.ok(json)
    }
  }
}