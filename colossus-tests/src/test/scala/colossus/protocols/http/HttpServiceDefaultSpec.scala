package colossus.protocols.http

import colossus.core.ServerRef
import colossus.protocols.http.HttpMethod.Get
import colossus.protocols.http.UrlParsing._
import colossus.service.GenRequestHandler.PartialHandler
import colossus.testkit.HttpServiceSpec

class HttpServiceDefaultSpec extends HttpServiceSpec {

  override def service: ServerRef = {
    HttpServer.start("example-server", 8123) { initContext =>
      new Initializer(initContext) {
        override def onConnect: RequestHandlerFactory =
          serverContext =>
            new RequestHandler(serverContext) {
              override def handle: PartialHandler[Http] = {
                case request @ Get on Root / "bang" => throw new Exception("Shouldn't have done that")
              }
          }
      }
    }
  }

  "Http service" must {
    "return not found on unhandled route" in {
      expectCodeAndBody(HttpRequest.get("ping"), HttpCodes.NOT_FOUND, "Not found")
    }

    "return server exception on unhandled exception" in {
      expectCodeAndBody(
        HttpRequest.get("bang"),
        HttpCodes.INTERNAL_SERVER_ERROR,
        "java.lang.Exception: Shouldn't have done that"
      )
    }
  }
}
