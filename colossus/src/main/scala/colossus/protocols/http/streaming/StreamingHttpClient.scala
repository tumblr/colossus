package colossus.protocols.http.streaming

import colossus.controller.{Controller, Encoding}
import colossus.core.{WorkerCommand, WorkerRef}
import colossus.protocols.http.BaseHttpClient
import colossus.service._
import colossus.streaming.Source

trait StreamingHttpClient
    extends LiftedClient[StreamingHttp, Callback]
    with BaseHttpClient[Callback, Source[Data], StreamingHttp] {

  val ops = StreamingHttpRequestOps
}

object StreamingHttpClient {

  val client = new ClientFactory[StreamingHttp, Callback, StreamingHttpClient, WorkerRef] {
    def defaultName = "streaming-http-client"

    override def apply(config: ClientConfig)(implicit worker: WorkerRef): StreamingHttpClient = {
      // TODO not sure how this will work under a load balancer
      config.address.map { address =>
        val client = new ServiceClient[StreamingHttp](address, config, worker.generateContext())
        val handler = new UnbindHandler(
          new Controller[Encoding.Client[StreamHttp]](
            new HttpStreamClientController(client),
            new StreamHttpClientCodec
          ),
          client
        )
        worker.worker ! WorkerCommand.Bind(handler)
        new BasicLiftedClient(client, Some(config))(implicitly[AsyncBuilder[Callback, WorkerRef]].build(worker))
        with StreamingHttpClient
      }
    }.head
  }

}
