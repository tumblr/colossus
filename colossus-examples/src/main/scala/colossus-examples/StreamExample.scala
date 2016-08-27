package colossus.examples

import colossus._
import core._
import controller._

import protocols.http._
import stream._

import scala.util.{Failure, Success, Try}

object StreamExample {

  def start(port: Int)(implicit sys: IOSystem) = {
    Server.start("stream", port){worker => new Initializer(worker) {
      def onConnect = c => {
        val h = new StreamServerHandler(c) {
          def handle(message: StreamHttpMessage[HttpRequestHead]) {
            message match {
              case Head(head) if (head.path == "/zop") => head.parameters.getFirstAs[Int]("num") match {
                case Success(num) => {
                  upstream.push (Head(HttpResponseHead(head.version, HttpCodes.OK, HttpHeaders.fromString("transfer-encoding" -> "chunked")))){_ => ()}
                  def sendNumbers(num: Int): Unit = num match {
                    case 0 => upstream.push(End()){_ => ()}
                    case n => {
                      upstream.push (BodyData(DataBlock(s"$n\r\n"))){_ => sendNumbers(n - 1)}
                    }
                  }
                  sendNumbers(num)
                }
                case Failure(reason) => {
                  upstream.pushCompleteMessage(HttpResponse.badRequest(reason.getMessage)){_ => ()}
                }
              }
              case Head(head) => upstream.pushCompleteMessage(HttpResponse.ok("Hello World!")){_ => ()}
              case _ => {}
            }
          }
        }
        new CoreHandler(new Controller(new ServerStreamController(h), new StreamHttpServerCodec), h)
      }
    }}
  }
}

