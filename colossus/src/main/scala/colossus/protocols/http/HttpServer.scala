package colossus
package protocols.http
package server

import core._
import controller._
import service._

protected[server] class HttpServiceHandler(rh: RequestHandler) 
extends ServiceServer[Http](rh) {


}


protected[server] class Generator(context: InitContext) extends HandlerGenerator[RequestHandler](context) {
  
  val DateHeader = new DateHeader
  val ServerHeader = HttpHeader("Server", context.server.name.idString)

  val defaultHeaders = HttpHeaders(DateHeader, ServerHeader)

  def fullHandler = requestHandler => new PipelineHandler(
    new Controller(
      new HttpServiceHandler(requestHandler),
      new StaticHttpServerCodec(defaultHeaders)
    ),
    requestHandler
  )

}

abstract class Initializer(ctx: InitContext) extends Generator(ctx) with ServiceInitializer[RequestHandler]


/**
 * A RequestHandler contains the business logic for transforming [[HttpRequest]] into [[HttpResponse]] objects.  
 */
abstract class RequestHandler(config: ServiceConfig, ctx: ServerContext) extends GenRequestHandler[Http](config, ctx) {
  def this(ctx: ServerContext) = this(ServiceConfig.load(ctx.name), ctx)

  val defaults = new Http.ServerDefaults

  override def tagDecorator = new ReturnCodeTagDecorator

  override def handleRequest(input: Http#Request): Callback[Http#Response] = {
    val response = super.handleRequest(input)
    if(!input.head.persistConnection) connection.disconnect()
    response
  }
  def unhandledError = {
    case error => defaults.errorResponse(error)
  }
}

/**
 * Entry point for starting a Http server
 */
object HttpServer extends ServiceDSL[RequestHandler, Initializer]{

  def basicInitializer = new Generator(_)
  
  def basic(name: String, port: Int)(handler: PartialFunction[HttpRequest, Callback[HttpResponse]])(implicit io: IOSystem) = start(name, port){new Initializer(_) {
    def onConnect = new RequestHandler(_) { def handle = handler }
  }}
}

