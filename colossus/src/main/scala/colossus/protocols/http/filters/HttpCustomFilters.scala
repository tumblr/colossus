package colossus.protocols.http.filters

import colossus.protocols.http.{ContentEncoding, Http, HttpBody, HttpHeaders}
import colossus.service.Filter
import colossus.service.GenRequestHandler.PartialHandler
import colossus.util.ZCompressor


object HttpCustomFilters {
  class GZipFilter extends Filter[Http] {
    override def apply(next: PartialHandler[Http]): PartialHandler[Http] = {
      case request =>
        next(request).map { response =>
          request.head.headers.firstValue(HttpHeaders.AcceptEncoding) match {
            case Some(header) if header.toLowerCase.contains(ContentEncoding.Gzip.value) =>
              val deflater = new ZCompressor()
              val compressed = deflater.compress(response.body.bytes)
              val headers = response.head.headers + (HttpHeaders.ContentEncoding, ContentEncoding.Gzip.value)
              val newHead = response.head.copy(headers = headers)

              response.copy(body = HttpBody(compressed), head = newHead)

            case _ =>
              response
          }
        }
    }
  }
}

