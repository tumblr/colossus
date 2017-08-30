import akka.actor.ActorSystem
import colossus.IOSystem
import colossus.protocols.http.HttpMethod.Get
import colossus.protocols.http.UrlParsing.{Root, on}
import colossus.protocols.http.{HttpServer, Initializer, RequestHandler}
import colossus.protocols.http.{Http, HttpClient, HttpRequest}
import colossus.service.GenRequestHandler.PartialHandler
import colossus.service.{Async, CallbackAsync, FutureAsync}

import scala.language.higherKinds

object GenericClient extends App {

  // #example
  implicit val actorSystem = ActorSystem()
  implicit val ioSystem    = IOSystem()

  def doTheThing[A[_]](client: HttpClient[A])(implicit async: Async[A]): A[String] = {
    async.map(client.send(HttpRequest.get("/"))) { response =>
      response.body.bytes.utf8String
    }
  }

  // using future client
  implicit val futureAsync      = new FutureAsync()
  implicit val executionContext = actorSystem.dispatcher
  doTheThing(Http.futureClient("example.org", 80)).map { string =>
    println(string)
  }

  HttpServer.start("example-server", 9000) { initContext =>
    new Initializer(initContext) {
      val client = Http.client("example.org", 80)

      override def onConnect = serverContext => new RequestHandler(serverContext) {
        override def handle: PartialHandler[Http] = {
          case request @ Get on Root =>
            // using callback client
            implicit val callbackAsync = CallbackAsync
            doTheThing(client).map { string =>
              request.ok(string)
            }
        }
      }
    }
  }
  // #example

}
