package colossus.examples

import colossus._
import core._
import protocols.http._
import stream._

import scala.util.{Failure, Success, Try}


object StreamExample {

  def start(port: Int)(implicit sys: IOSystem) = {
    Server.start("stream", port){worker => new Initializer(worker) {
      def onConnect = new StreamServerHandler(_) {

        def processMessage(message: StreamHttpRequest) {
          message match {
            case RequestHead(head) => head.parameters.getFirstAs[Int]("num") match {
              case Success(num) => {
                push (ResponseHead(HttpResponseHead(head.version, HttpCodes.OK, HttpHeaders.fromString("transfer-encoding" -> "chunked")))){_ => ()}
                def sendNumbers(num: Int): Unit = num match {
                  case 0 => push(End){_ => ()}
                  case n => {
                    push (BodyData(DataBlock(s"$n\r\n"))){_ => sendNumbers(n - 1)}
                  }
                }
                sendNumbers(num)
              }
              case Failure(reason) => {
                pushResponse(HttpResponse.badRequest(reason.getMessage)){_ => ()}
              }
            }
            case _ => {}
          }
        }
      }
    }}
  }
}
