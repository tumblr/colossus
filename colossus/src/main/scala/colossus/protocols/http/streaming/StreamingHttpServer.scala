package colossus.protocols.http.streaming

import colossus.controller.{Controller, Encoding}
import colossus.core.{InitContext, PipelineHandler, ServerConnectionHandler}
import colossus.service._

class StreamingHttpServiceHandler(rh: GenRequestHandler[StreamingHttp]) extends ServiceServer[StreamingHttp](rh)

class StreamServiceHandlerGenerator(ctx: InitContext) extends HandlerGenerator[GenRequestHandler[StreamingHttp]](ctx) {

  def fullHandler = handler => {
    new PipelineHandler(
      new Controller[Encoding.Server[StreamHttp]](
        new HttpStreamServerController(
          new StreamingHttpServiceHandler(handler)
        ),
        new StreamHttpServerCodec
      ),
      handler
    ) with ServerConnectionHandler
  }
}

abstract class StreamServiceInitializer(ctx: InitContext)
    extends StreamServiceHandlerGenerator(ctx)
    with ServiceInitializer[GenRequestHandler[StreamingHttp]]

object StreamingHttpServer extends ServiceDSL[GenRequestHandler[StreamingHttp], StreamServiceInitializer] {
  def basicInitializer = initContext => new StreamServiceHandlerGenerator(initContext)
}
