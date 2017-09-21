package colossus.protocols.http

import colossus.IOSystem
import colossus.controller.Controller
import colossus.core.{InitContext, PipelineHandler, ServerContext}
import colossus.service._

class HttpServiceHandler(rh: RequestHandler) extends ServiceServer[Http](rh) {}

class Generator(context: InitContext) extends HandlerGenerator[RequestHandler](context) {

  val DateHeader   = new DateHeader
  val ServerHeader = HttpHeader("Server", context.server.name.idString)

  val defaultHeaders = HttpHeaders(DateHeader, ServerHeader)

  def fullHandler =
    requestHandler =>
      new PipelineHandler(
        new Controller(
          new HttpServiceHandler(requestHandler),
          new StaticHttpServerCodec(defaultHeaders, requestHandler.config.maxRequestSize)
        ),
        requestHandler
    )

}

abstract class Initializer(ctx: InitContext) extends Generator(ctx) with ServiceInitializer[RequestHandler]

/**
  * A RequestHandler contains the business logic for transforming [[HttpRequest]] into [[HttpResponse]] objects.
  */
abstract class RequestHandler(ctx: ServerContext, config: ServiceConfig) extends GenRequestHandler[Http](ctx, config) {
  def this(ctx: ServerContext) = this(ctx, ServiceConfig.load(ctx.name))

  val defaults = new Http.ServerDefaults

  override def tagDecorator = new ReturnCodeTagDecorator

  override def handleRequest(input: Http#Request): Callback[Http#Response] = {
    val response = super.handleRequest(input)
    if (!input.head.persistConnection) connection.disconnect()
    response
  }
  def unhandledError = {
    case error => defaults.errorResponse(error)
  }
}

/**
  * Entry point for starting a Http server
  */
object HttpServer extends ServiceDSL[RequestHandler, Initializer] {

  def basicInitializer = initContext => new Generator(initContext)

  def basic(name: String, port: Int)(handler: PartialFunction[HttpRequest, Callback[HttpResponse]])(
      implicit io: IOSystem) = start(name, port) { initContext =>
    new Initializer(initContext) {
      def onConnect = serverContext => new RequestHandler(serverContext) { def handle = handler }
    }
  }
}
