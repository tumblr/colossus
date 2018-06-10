package colossus.protocols.http.streaming

import colossus.controller.{Controller, Encoding}
import colossus.core.{WorkerCommand, WorkerRef}
import colossus.protocols.http.{BaseHttpClient, HttpHeader, HttpHeaders}
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

    def requestEnhancement: (StreamingHttpRequest, Sender[StreamingHttp, Callback]) => StreamingHttpRequest =
      (input: StreamingHttpRequest, sender: Sender[StreamingHttp, Callback]) => {
        input.head.headers.firstValueInternal(HttpHeaders.Host) match {
          case Some(_) => input // Host header is already present.
          case None =>
            StreamingHttpRequestOps.withHeader(input, HttpHeader(HttpHeaders.Host, sender.address().getHostName))
        }
      }

    override def apply(config: ClientConfig)(implicit worker: WorkerRef): StreamingHttpClient = {
      if (config.address.size > 1) {
        throw new Exception("Streaming http client not designed to be load balanced")
      } else {
        config.address.map { address =>
          val client = new ServiceClient[StreamingHttp](address, config, worker.generateContext(), requestEnhancement)
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

}
