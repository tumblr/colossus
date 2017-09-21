package colossus.protocols.http.streaming

import colossus.protocols.http.{BaseHttp, BaseHttpMessage, HttpMessageHead, HttpRequestHead, HttpResponse, HttpResponseHead}
import colossus.streaming.Source

trait StreamingHttpMessage[T <: HttpMessageHead] extends BaseHttpMessage[T, Source[Data]] {

  def collapse: Source[HttpStream[T]] = Source.one[HttpStream[T]](Head(head)) ++ body ++ Source.one[HttpStream[T]](End)
}

case class StreamingHttpRequest(head: HttpRequestHead, body: Source[Data])
    extends StreamingHttpMessage[HttpRequestHead]

class StreamingHttpResponse(val head: HttpResponseHead, val body: Source[Data])
    extends StreamingHttpMessage[HttpResponseHead]

object StreamingHttpResponse {

  def apply(head: HttpResponseHead, body: Source[Data]): StreamingHttpResponse = new StreamingHttpResponse(head, body)

  def apply(response: HttpResponse): StreamingHttpResponse =
    new StreamingHttpResponse(response.head, Source.one(Data(response.body.asDataBlock))) {
      override def collapse = Source.fromArray(Array(Head(head), Data(response.body.asDataBlock), End))
    }
}

trait StreamingHttp extends BaseHttp[Source[Data]] {
  type Request  = StreamingHttpRequest
  type Response = StreamingHttpResponse
}
