// #hello_world_2
import akka.actor.ActorSystem
import colossus.IOSystem
import colossus.core.{InitContext, ServerContext}
import colossus.protocols.http.Http
import colossus.protocols.http.HttpMethod._
import colossus.protocols.http.UrlParsing._
import colossus.protocols.http.{HttpServer, Initializer, RequestHandler}
import colossus.service.Callback
import colossus.service.GenRequestHandler.PartialHandler

object HelloWorld2 extends App {

  // #hello_world_part3
  implicit val actorSystem = ActorSystem()
  implicit val ioSystem    = IOSystem()
  // #hello_world_part3

  // #hello_world_part4
  HttpServer.start("hello-world", 9000) { context =>
    new HelloInitializer(context)
  }
  // #hello_world_part4

}

// #hello_world_part2
class HelloInitializer(context: InitContext) extends Initializer(context) {

  override def onConnect = context => new HelloRequestHandler(context)

}
// #hello_world_part2

// #hello_world_part1
class HelloRequestHandler(context: ServerContext) extends RequestHandler(context) {

  override def handle: PartialHandler[Http] = {
    case request @ Get on Root / "hello" => {
      Callback.successful(request.ok("Hello World!"))
    }
  }

}
// #hello_world_part1
// #hello_world_2
