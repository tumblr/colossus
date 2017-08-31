import akka.actor.ActorSystem
import colossus.IOSystem
import colossus.protocols.http.Http
import colossus.protocols.http.HttpMethod.Get
import colossus.protocols.http.UrlParsing.{Root, on}
import colossus.protocols.http.{HttpServer, Initializer, RequestHandler}
import colossus.service.Callback
import colossus.service.GenRequestHandler.PartialHandler

object WorkerExample extends App {
  implicit val actorSystem = ActorSystem()
  implicit val ioSystem    = IOSystem()

  // #example
  case class NameChange(name: String)

  val serverRef = HttpServer.start("example-server", 9000) { initContext =>
    new Initializer(initContext) {

      var currentName = "Jones"

      override def receive = {
        case NameChange(name) => currentName = name
      }

      override def onConnect = serverContext => new RequestHandler(serverContext) {
        override def handle: PartialHandler[Http] = {
          case request @ Get on Root => Callback.successful(request.ok(s"My name is $currentName"))
        }
      }
    }
  }

  serverRef.initializerBroadcast(NameChange("Smith"))
  // #example
}
