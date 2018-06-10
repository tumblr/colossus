package colossus.protocols.http.filters

import colossus.protocols.http.{ContentEncoding, HttpHeaders, HttpRequestHead}
import colossus.util.compress.{Compressor, GzipCompressor, ZCompressor}

object FilterHeaderUtils {

  def getCompressorFromHeader(head: HttpRequestHead, bufferedKb: Int): Option[(Compressor, ContentEncoding)] = {
    head.headers.firstValueInternal(HttpHeaders.AcceptEncoding).flatMap { header =>
      if (header.contains(ContentEncoding.Gzip.value)) {
        Some(new GzipCompressor(bufferedKb), ContentEncoding.Gzip)
      } else if (header.contains(ContentEncoding.Deflate.value)) {
        Some(new ZCompressor(bufferedKb), ContentEncoding.Deflate)
      } else {
        None
      }
    }
  }
}
