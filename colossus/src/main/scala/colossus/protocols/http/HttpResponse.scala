package colossus
package protocols.http

import akka.util.{ByteString, ByteStringBuilder}
import colossus.controller.Source
import colossus.core.DataBuffer
import core._
import service.Callback
import controller._
import HttpParse._

case class HttpResponseHeader(key: ByteString, value: ByteString)

object HttpResponseHeader {

  //TODO: these are bytestrings, whereas in Head they're strings
  val ContentLength = ByteString("content-length")
  val TransferEncoding = ByteString("transfer-encoding")

  val DELIM = ByteString(": ")

  def apply(key: String, value: String): HttpResponseHeader = {
    HttpResponseHeader(ByteString(key) , ByteString(value))
  }


  object Conversions {
    implicit def stringTuple2Header(t: (String, String)): HttpResponseHeader = HttpResponseHeader(t._1, t._2)
    implicit def seqStringTuple2Headers(t: Seq[(String, String)]): Vector[HttpResponseHeader] = t.map{stringTuple2Header}.toVector
  }

}

case class HttpResponseHead(version : HttpVersion, code : HttpCode, headers : Vector[HttpResponseHeader] = Vector()) {

  def appendHeaderBytes(builder: ByteStringBuilder) =  {
    builder append version.messageBytes
    builder putByte ' '
    builder append code.headerBytes
    builder append NEWLINE
    headers.foreach{case header =>
      builder ++= header.key
      builder ++= HttpResponseHeader.DELIM
      builder ++= header.value
      builder ++= NEWLINE
    }
  }

  def withHeader(key: String, value: String) = copy(headers = headers :+ HttpResponseHeader(key, value))

  def contentLength = {
    headers.collectFirst{case HttpResponseHeader(HttpResponseHeader.ContentLength, v) => v.utf8String.toInt}
  }

  def transferEncoding : TransferEncoding = {
    headers.collectFirst{case HttpResponseHeader(HttpResponseHeader.TransferEncoding, v) => v}.flatMap{t => TransferEncoding.unapply(t.utf8String)}.getOrElse(TransferEncoding.Identity)
  }

}

//TODO: all of this should be generalized for requests and responses

sealed trait HttpMessageBody {
  def resolve: Callback[ByteString]
}
object HttpMessageBody {
  case object NoBody extends HttpMessageBody {
    def resolve = Callback.successful(ByteString())
  }
  case class Data(data: ByteString) extends HttpMessageBody {
    def resolve = Callback.successful(data)
  }
  case class Stream(data: Source[DataBuffer]) extends HttpMessageBody {
    def resolve = data.fold(new ByteStringBuilder) { case (data, builder) => builder.putBytes(data.takeAll); builder}.map{_.result}
  }

  //maybe this isn't needed
  /*
  case class ChunkStream(chunks: source[HttpChunk]) extends HttpMessageBody {
    def resolve = data.fold(new ByteStringBuilder) { case (chunk, builder) => builder.append(chunk.data); builder}.map{_.result}
  }
  */

  implicit def liftBytes(data: ByteString): HttpMessageBody = Data(data)
  implicit def liftStream(stream: Source[DataBuffer]) = DataStream(stream)
}

//TODO: We need to make some headers like Content-Length, Transfer-Encoding,
//first-class citizens and separate them from the other headers.  This would
//prevent things like creating a response with the wrong content length

case class HttpResponse(head: HttpResponseHead, body: HttpMessageBody) {
  import HttpMessageBody._

  
  def encodeHead(contentLength: Option[Int]): ByteStringBuilder = {
    val s = contentLength.getOrElse(0)
    val builder = new ByteStringBuilder
    builder.sizeHint((50 * head.headers.size) + s)
    head.appendHeaderBytes(builder)
    contentLength.foreach{c => 
      builder append HttpResponse.ContentLengthKey
      builder putBytes c.toString.getBytes
      builder append NEWLINE
    }
    builder append NEWLINE

  }

  def encode() : DataReader = body match {
    case Data(data: ByteString) => {
      val builder = encodeHead(Some(data.size))
      builder append data
      DataBuffer(builder.result())
    }
    case NoBody => {
      val builder = encodeHead(Some(0))
      DataBuffer(builder.result())      
    }
    case Stream(data: Source[DataBuffer]) => {
      val head = DataBuffer(encodeHead(None).result)
      DataStream(new DualSource(Source.one(head), data))
    }
  }

  def withHeader(key: String, value: String) = copy(head = head.withHeader(key,value))

  def code = head.code

}

/**
  * Converter typeclass for bytestrings.  Default implementations are in package.scala
  */
trait ByteStringLike[T] {

  def toByteString(t : T) : ByteString

}

object HttpResponse {

  val ContentLengthKey = ByteString("Content-Length: ")

  def apply[T : ByteStringLike](version : HttpVersion, code : HttpCode, headers : Vector[HttpResponseHeader] , data : T) : HttpResponse = {
    HttpResponse(HttpResponseHead(version, code, headers), HttpMessageBody.Data(implicitly[ByteStringLike[T]].toByteString(data)))
  }


  def apply(version : HttpVersion, code : HttpCode, headers : Vector[HttpResponseHeader]) : HttpResponse = {
    HttpResponse(HttpResponseHead(version, code, headers), HttpMessageBody.NoBody)
  }

}

