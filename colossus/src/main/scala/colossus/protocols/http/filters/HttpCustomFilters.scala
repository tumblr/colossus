package colossus.protocols.http.filters

import colossus.protocols.http.{ContentEncoding, Http, HttpBody, HttpHeaders}
import colossus.service.Filter
import colossus.service.GenRequestHandler.PartialHandler
import colossus.util.{GzipCompressor, ZCompressor}


object HttpCustomFilters {

  class CompressionFilter(bufferedKb: Int = 10) extends Filter[Http] {

    override def apply(next: PartialHandler[Http]): PartialHandler[Http] = {
      case request =>
        val maybeCompressor = request.head.headers.firstValue(HttpHeaders.AcceptEncoding).flatMap { header =>
          if (header.contains(ContentEncoding.Gzip.value)) {
            Some(new GzipCompressor(bufferedKb), ContentEncoding.Gzip)
          } else if (header.contains(ContentEncoding.Deflate.value)) {
            Some(new ZCompressor(bufferedKb), ContentEncoding.Deflate)
          } else {
            None
          }
        }

        next(request).map { response =>
          maybeCompressor match {
            case Some((compressor, encoding)) =>
              val compressed = compressor.compress(response.body.bytes)
              val headers = response.head.headers + (HttpHeaders.ContentEncoding, encoding.value)
              val newHead = response.head.copy(headers = headers)
              response.copy(body = HttpBody(compressed), head = newHead)
            case None =>
              response
          }
        }
    }
  }

}
