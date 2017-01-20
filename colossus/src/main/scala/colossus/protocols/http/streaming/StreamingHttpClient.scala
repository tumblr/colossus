package colossus
package protocols.http
package streaming

import controller._
import core.{WorkerCommand, WorkerRef}

import service._
import colossus.streaming.Source

trait StreamingHttpClient extends LiftedClient[StreamingHttp, Callback] with BaseHttpClient[Callback, Source[Data], StreamingHttp] {
  
  val ops = StreamingHttpRequestOps
}

object StreamingHttpClient {
    
  val client = new ClientFactory[StreamingHttp, Callback, StreamingHttpClient, WorkerRef] {
    def defaultName = "streaming-http-client"

    def apply(config: ClientConfig)(implicit worker: WorkerRef): StreamingHttpClient = {
      val client = new ServiceClient[StreamingHttp](config, worker.generateContext())
      val handler = new UnbindHandler(
        new Controller[Encoding.Client[StreamHttp]](
          new HttpStreamClientController(client),
          new StreamHttpClientCodec
        ), 
        client
      )
      worker.worker ! WorkerCommand.Bind(handler)
      new BasicLiftedClient(client, Some(config))(implicitly[AsyncBuilder[Callback, WorkerRef]].build(worker)) with StreamingHttpClient
    }
  }

}
