import akka.actor.ActorSystem
import colossus.IOSystem
import colossus.protocols.http.HttpMethod.Get
import colossus.protocols.http.{Http, HttpServer, Initializer, RequestHandler}
import colossus.protocols.http.UrlParsing.{Root, on}
import colossus.service.Filter
import colossus.service.GenRequestHandler.PartialHandler
import colossus.service.Callback.Implicits._

object FilterExample extends App {

  implicit val actorSystem = ActorSystem()
  implicit val ioSystem    = IOSystem()

  HttpServer.start("example-server", 9000) { initContext =>
    new Initializer(initContext) {
      override def onConnect =
        serverContext =>
          new RequestHandler(serverContext) {
            override def handle: PartialHandler[Http] = {
              case request @ Get on Root => request.ok("Hello World!")
            }
            // #example1
            override def filters: Seq[Filter[Http]] = Seq(
              new AllowedHostsFilter(),
              new ReverseResponseFilter()
            )
            // #example1
        }
    }
  }

  // #example
  /**
    * Primitive allowed hosts filter
    */
  class AllowedHostsFilter extends Filter[Http] {
    override def apply(next: PartialHandler[Http]): PartialHandler[Http] = {
      case request =>
        request.head.headers.firstValue("Host") match {
          case Some("localhost:9011") => next(request)
          case Some(host)             => request.error(s"host $host not allowed")
          case None                   => request.error("no host header")
        }
    }
  }

  /**
    * filter to reverse response
    */
  class ReverseResponseFilter extends Filter[Http] {
    override def apply(next: PartialHandler[Http]): PartialHandler[Http] = {
      case request =>
        next(request).flatMap { response =>
          request.ok(response.body.toString.reverse, response.head.headers)
        }
    }
  }
  // #example

}
