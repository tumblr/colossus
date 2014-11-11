package colossus
package protocols

import service._

package object http {

  class HttpParsingException(message: String) extends Exception(message)

  class InvalidRequestException(message: String) extends Exception(message)



  trait Http extends CodecDSL {
    type Input = HttpRequest
    type Output = HttpResponse
  }

  implicit object HttpProvider extends CodecProvider[Http] {
    def provideCodec = new HttpServerCodec
    def errorResponse(request: HttpRequest, reason: Throwable) = request.error(reason.toString)

  }

  implicit object HttpClientProvider extends ClientCodecProvider[Http] {
    def clientCodec = new HttpClientCodec
    def name = "http"
  }

}
