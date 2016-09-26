package colossus.examples

import colossus._

import protocols.http._
import stream._
import core.{DataBlock, ServerContext}
import controller._

import scala.util.{Failure, Success, Try}

object StreamExample {

  class Handler(ctx: ServerContext) extends StreamServerHandler(ctx) {
    def handle(message: HttpStream[HttpRequestHead]) {
      message match {
        case Head(head) if (head.path == "/zop") => head.parameters.getFirstAs[Int]("num") match {
          case Success(num) => {
            upstream.push (Head(HttpResponseHead(head.version, HttpCodes.OK, HttpHeaders.fromString("transfer-encoding" -> "chunked")))){_ => ()}
            def sendNumbers(num: Int): Unit = num match {
              case 0 => upstream.push(End){_ => ()}
              case n => {
                upstream.push (Data(DataBlock(s"$n\r\n"))){_ => sendNumbers(n - 1)}
              }
            }
            sendNumbers(num)
          }
          case Failure(reason) => {
            upstream.pushCompleteMessage(HttpResponse.badRequest(reason.getMessage)){_ => ()}
          }
        }
        case Head(head) if (head.path == "/") => upstream.pushCompleteMessage(HttpResponse.ok("Hello World!")){_ => ()}
        case Head(_) => {
          upstream.pushCompleteMessage(HttpResponse.notFound("Bye!")){_ => shutdown()}
        }
        case _ => {}
      }
    }
  }

  def start(port: Int)(implicit sys: IOSystem) = {
    StreamHttpServer.basic("stream", port, new Handler(_))
  }
}

