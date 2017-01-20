package colossus
package protocols.http
package streaming

import controller._
import core._
import service._

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
  extends StreamServiceHandlerGenerator(ctx) with ServiceInitializer[GenRequestHandler[StreamingHttp]] 

object StreamingHttpServer extends ServiceDSL[GenRequestHandler[StreamingHttp], StreamServiceInitializer] {
  def basicInitializer = new StreamServiceHandlerGenerator(_)
}
