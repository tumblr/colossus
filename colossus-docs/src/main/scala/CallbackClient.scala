import akka.actor.ActorSystem
import colossus.IOSystem
import colossus.protocols.http.{Http, HttpRequest}
import colossus.protocols.http.HttpMethod.Get
import colossus.protocols.http.UrlParsing.{Root, on}
import colossus.protocols.http.{HttpServer, Initializer, RequestHandler}
import colossus.service.GenRequestHandler.PartialHandler

object CallbackClient extends App {

  // #callback_client
  implicit val actorSystem = ActorSystem()
  implicit val ioSystem    = IOSystem()

  HttpServer.start("example-server", 9000) { initContext =>
    new Initializer(initContext) {

      val callbackClient = Http.client("example.org", 80)

      override def onConnect = serverContext => new RequestHandler(serverContext) {
        override def handle: PartialHandler[Http] = {
          case request @ Get on Root =>
            val request = HttpRequest.get("/")
            callbackClient.send(request)
        }
      }
    }
  }
  // #callback_client
}
