package colossus.examples

import colossus.service.Callback.Implicits._
import colossus.IOSystem
import colossus.core.{ServerContext, ServerRef}
import colossus.protocols.http.HttpMethod.Get
import colossus.protocols.http.{Http, HttpServer, Initializer, RequestHandler}
import colossus.protocols.http.UrlParsing.{/, Root, on}
import colossus.service.GenRequestHandler.PartialHandler
import colossus.service.Filter

object HttpFilterExample {

  class HttpFilterExampleHandler(context: ServerContext) extends RequestHandler(context) {

    def handle = {
      case req @ Get on Root => req.ok("Hello World!")
    }

    override def filters = Seq(
      new AllowedHostsFilter(),
      new ReverseResponseFilter()
    )

  }

  def start(port: Int)(implicit system: IOSystem): ServerRef = {
    HttpServer.start("http-filter-example", port) { init =>
      new Initializer(init) {
        def onConnect = context => new HttpFilterExampleHandler(context)
      }
    }
  }

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
    * meaningless filter to demonstrate response modification (gzip for example)
    */
  class ReverseResponseFilter extends Filter[Http] {
    override def apply(next: PartialHandler[Http]): PartialHandler[Http] = {
      case request =>
        next(request).flatMap { response =>
          request.ok(response.body.toString.reverse, response.head.headers)
        }
    }
  }
}
