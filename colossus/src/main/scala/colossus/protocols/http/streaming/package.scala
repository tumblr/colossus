package colossus.protocols.http

import colossus.streaming.Source

package object streaming {

  implicit object StreamingHttpRequestOps extends MessageOps[HttpRequestHead, Source[Data], StreamingHttpRequest](StreamingHttpRequest.apply _)

}
