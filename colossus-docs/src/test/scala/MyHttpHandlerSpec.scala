import colossus.core.{ServerContext, ServerRef}
import colossus.protocols.http.HttpMethod.Get
import colossus.protocols.http.{ContentType, Http, HttpCodes, HttpRequest, HttpServer, Initializer, RequestHandler}
import colossus.protocols.http.UrlParsing.{/, Root, on}
import colossus.service.Callback
import colossus.service.GenRequestHandler.PartialHandler
import colossus.testkit.HttpServiceSpec
import org.json4s.Formats
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods

// to run this: start sbt, set project to colossus-docs, and run "testOnly TestkitExampleSpec".

// #example2
class MyHttpHandlerSpec extends HttpServiceSpec {

  implicit val formats: Formats = org.json4s.DefaultFormats

  override def service: ServerRef = {
    HttpServer.start("example-server", 9123) { initContext =>
      new Initializer(initContext) {
        override def onConnect: ServerContext => MyHandler = { serverContext =>
          new MyHandler(serverContext)
        }
      }
    }
  }

  "My request handler" must {
    "return 200 and correct body" in {
      expectCodeAndBody(HttpRequest.get("ping"), HttpCodes.OK, "pong")
    }

    "return 200 and body that satisfies predicate" in {
      expectCodeAndBodyPredicate(HttpRequest.get("ping/1"), HttpCodes.OK) { body =>
        val actual   = JsonMethods.parse(body).extract[Map[String, JValue]]
        val expected = Map("data" -> JInt(1), "type" -> JString("pong"))
        actual == expected
      }
    }

  }

}
// #example2

// #example1
class MyHandler(context: ServerContext) extends RequestHandler(context) {
  override def handle: PartialHandler[Http] = {
    case request @ Get on Root / "ping" =>
      Callback.successful(request.ok("pong"))

    case request @ Get on Root / "ping" / data =>
      Callback.successful(request.ok(s"""{"type":"pong","data":$data}""").withContentType(ContentType.ApplicationJson))
  }
}
// #example1
