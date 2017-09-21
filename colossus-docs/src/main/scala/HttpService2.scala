import akka.actor.ActorSystem
import colossus.IOSystem
import colossus.protocols.http._
import colossus.protocols.http.HttpMethod._
import colossus.protocols.http.UrlParsing._
import colossus.protocols.http.{HttpServer, Initializer, RequestHandler}
import colossus.protocols.http.{ContentType, Http, HttpBody, HttpCodes, HttpHeader, HttpHeaders}
import colossus.service.GenRequestHandler.PartialHandler
import colossus.service.Callback.Implicits._

object HttpService2 {

  implicit val actorSystem = ActorSystem()
  implicit val ioSystem    = IOSystem()

  HttpServer.start("example-server", 9000) { initContext =>
    new Initializer(initContext) {
      override def onConnect = serverContext => new RequestHandler(serverContext) {
        override def handle: PartialHandler[Http] = {
          case request @ Get on Root =>
            // #example1
            request.ok("hello").withHeader("header-name", "header-value")
            request.ok("hello", HttpHeaders(HttpHeader("header-name", "header-value")))
            request.ok("hello", HttpHeaders(HttpHeader(HttpHeaders.CookieHeader, "header-value")))
          // #example1
            // #example1a
            request.ok("hello", HttpHeaders(HttpHeader("Content-Type", "header-value")))
            // #example1a

          case request @ Get on Root =>
            // #example3
            val body: String                = request.body.bytes.utf8String
            val contentType: Option[String] = request.head.headers.contentType
            val headers: HttpHeaders        = request.head.headers
            val parameter: Option[String]   = request.head.parameters.getFirst("key")
            // #example3

            // #example2
            request.respond(
              HttpCodes.CONFLICT,
              HttpBody("""{"name":"value"}""")
            ).withContentType(ContentType.ApplicationJson)
          // #example2
        }
      }
    }
  }

}
