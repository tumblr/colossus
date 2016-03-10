package colossus
package protocols.http

import akka.util.ByteString

import core.DataBuffer
import parsing._
import DataSize._
import HttpParse._
import Combinators._

object HttpRequestParser {
  import HttpBody._

  val DefaultMaxSize: DataSize = 1.MB

  def apply(size: DataSize = DefaultMaxSize) = maxSize(size, httpRequest)

  //TODO : don't parse body as a bytestring
  protected def httpRequest: Parser[HttpRequest] = httpHead |> {case HeadResult(head, contentLength, transferEncoding) => 
    transferEncoding match { 
      case None | Some("identity") => contentLength match {
        case Some(0) | None => const(HttpRequest(head, HttpBody.NoBody))
        case Some(n) => bytes(n) >> {body => HttpRequest(head, HttpBody(body))}
      }
      case Some(other)  => chunkedBody >> {body => HttpRequest(head, HttpBody(body))}
    } 
  }

  protected def httpHead = new HttpHeadParser
  
}

case class HeadResult(head: HttpRequestHead, contentLength: Option[Int], transferEncoding: Option[String] )

/**
 * This parser is optimized to reduce the number of operations per character
 * read
 */
class HttpHeadParser extends Parser[HeadResult]{

  class RBuilder {
    var method: String = ""
    var path: String = ""
    var version: String = ""
    var headers = new java.util.ArrayList[HttpHeader]
    var contentLength: Option[Int] = None
    var transferEncoding: Option[String] = None

    def addHeader(name: String, value: String) {
      headers add new DecodedHeader(name, value)
      if (name == HttpHeaders.ContentLength) {
        contentLength = Some(value.toInt)
      } else if (name == HttpHeaders.TransferEncoding) {
        transferEncoding = Some(value)
      }
    }

    def build: HeadResult = {
      val r = HeadResult(
        HttpRequestHead(
          HttpMethod(method),
          path,
          HttpVersion(version),
          new HttpHeaders(headers.toArray.asInstanceOf[Array[HttpHeader]])
        ), 
        contentLength,
        transferEncoding
      )
      reset()
      r
    }

    def reset() {
      headers = new java.util.ArrayList[HttpHeader]
      contentLength = None
      transferEncoding = None
    }
  }
  var requestBuilder = new RBuilder
  var headerState = 0 //incremented when parsing \r\n\r\n


  trait MiniParser {
    def parse(c: Char)
    def end()
  }

  class FirstLineParser extends MiniParser {
    val STATE_METHOD  = 0
    val STATE_PATH    = 1
    val STATE_VERSION = 2
    var state = STATE_METHOD
    val builder = new StringBuilder
    def parse(c: Char) {
      if (c == ' ') {
        val res = builder.toString
        builder.setLength(0)
        state match {
          case STATE_METHOD => {
            requestBuilder.method = res
            state = STATE_PATH
          }
          case STATE_PATH   => {
            requestBuilder.path = res
            state = STATE_VERSION
          }
          case _ => {
            throw new ParseException("invalid content in header first line")
          }
        }
      } else {
        builder.append(c)
      }
    }
    def end() {
      val res = builder.toString
      builder.setLength(0)
      requestBuilder.version = res
      state = STATE_METHOD
    }
  }

  class HeaderParser extends MiniParser {
    val STATE_KEY   = 0
    val STATE_VALUE = 1
    val STATE_TRIM = 2
    var state = STATE_KEY
    val builder = new StringBuilder
    var builtKey = ""
    def parse(c: Char) {
      state match {
        case STATE_KEY => {
          if (c == ':') {
            builtKey = builder.toString
            builder.setLength(0)
            state = STATE_TRIM
          } else {
            if (c >= 'A' && c <= 'Z') {
              builder.append((c + 32).toChar)
            } else {
              builder.append(c)
            }
          }
        }
        case STATE_TRIM => {
          if (c != ' ') {
            state = STATE_VALUE
            builder.append(c)
          }
        }
        case STATE_VALUE => {
          builder.append(c)
        }
      }
    }
    def end() {
      requestBuilder.addHeader(builtKey, builder.toString)
      builder.setLength(0)
      state = STATE_KEY
    }
  }

  val fparser = new FirstLineParser
  val hparser = new HeaderParser
        
  var currentParser: MiniParser = fparser


  def parse(d: DataBuffer): Option[HeadResult] = {
    while (d.hasUnreadData) {
      val b = d.next.toChar
      if (b == '\r') {
        headerState += 1
      } else if (b == '\n') {
        headerState += 1
        if (headerState == 2) {
          //finished reading in a line
          currentParser.end()
          if (currentParser == fparser) {
            currentParser = hparser
          }
        } else if (headerState == 4) {
          //two consecutive \r\n indicates the end of the request head
          currentParser = fparser
          headerState = 0
          return Some(requestBuilder.build)
        }
      } else {
        currentParser.parse(b)
        headerState = 0
      }
    }
    None

  }


}


