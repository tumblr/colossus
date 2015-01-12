package colossus
package protocols.http

import akka.util.{ByteString, ByteStringBuilder}
import colossus.controller.Pipe
import colossus.core.DataBuffer

import parsing._
import Combinators._

object HttpParse {

  val NEWLINE = ByteString("\r\n")

  //common parsers
  def headers: Parser[Seq[(String, String)]]    = repeatUntil(header, '\r') <~ byte
  def header: Parser[(String, String)]     = {
    stringUntil(':', toLower = true, allowWhiteSpace = false) ~ 
    stringUntil('\r', allowWhiteSpace = true, ltrim = true) <~ 
    byte >> {case key ~ value => (key, value)}
  }

  def chunkedBody: Parser[ByteString] = chunkedBodyBuilder(new ByteStringBuilder)

  private def chunkedBodyBuilder(builder: ByteStringBuilder): Parser[ByteString] = intUntil('\r', 16) <~ byte |> {
    case 0 => bytes(2) >> {_ => builder.result}
    case n => bytes(n.toInt) <~ bytes(2) |> {bytes => chunkedBodyBuilder(builder.append(bytes))}
  }
}

object StreamingHttpChunkPipe {

  private val chunkedBodyParser: Parser[ByteString] = intUntil('\r', 16) <~ byte |> {
    case 0 => bytes(2) >> { _ => ByteString("")}
    case n => bytes(n.toInt) <~ bytes(2) >> { bytes => bytes}
  }

  private val streamTerminator = ByteString("")

  def convertChunks(buffer : DataBuffer): Seq[DataBuffer] = {
    val builder = new ByteStringBuilder()
    var chunks : Vector[DataBuffer] = Vector()
    var bodyFinished : Boolean = false
    while (buffer.hasUnreadData && !bodyFinished) { //should buffer have a fold function?
      val res = chunkedBodyParser.parse(buffer)
      res match {
        case Some(`streamTerminator`) => {
          chunks = chunks :+ DataBuffer(builder.result())
          bodyFinished = true
        }
        case Some(x) => builder.append(x)
        case None => {}
      }
    }
    chunks
  }

  def apply(src : Pipe[DataBuffer, DataBuffer], sink : Pipe[DataBuffer, DataBuffer]) : Pipe[DataBuffer, DataBuffer] = {
    src.join(sink)(convertChunks)
  }
}

