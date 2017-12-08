package colossus.protocols.http.filters

import akka.util.ByteString
import colossus.core.DataBlock
import colossus.protocols.http.streaming.{Data, StreamingHttp, StreamingHttpResponse}
import colossus.protocols.http.{HttpHeaders, TransferEncoding}
import colossus.service.GenRequestHandler.PartialHandler
import colossus.service.{Callback, Filter}
import colossus.streaming.Source

object HttpStreamCustomFilters {

  class CompressionFilter(bufferedKb: Int = 10) extends Filter[StreamingHttp] {

    override def apply(next: PartialHandler[StreamingHttp]): PartialHandler[StreamingHttp] = {
      case request =>
        val maybeCompressor = FilterHeaderUtils.getCompressorFromHeader(request.head, bufferedKb)

        next(request).flatMap { response =>
          maybeCompressor match {
            case Some((compressor, encoding)) =>
              val iteratorCB: Callback[Iterator[Data]] = response.body
                .fold(new collection.mutable.ArrayBuffer[Data]) {
                  case (data, buffer) =>
                    val bytes      = ByteString(data.data.data)
                    val compressed = compressor.compress(bytes).toArray
                    val newData    = Data(DataBlock(compressed))
                    buffer.append(newData)
                    buffer
                }
                .map(_.toIterator)

              val headers = response.head.headers +
                (HttpHeaders.ContentEncoding, encoding.value) +
                (HttpHeaders.TransferEncoding, TransferEncoding.Chunked.value)

              val newHead = response.head.copy(headers = headers)
              iteratorCB.map(iter => StreamingHttpResponse(newHead, Source.fromIterator(iter)))

            case None => Callback.successful(response)
          }
        }
    }
  }

}
