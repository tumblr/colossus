package colossus
package service

import colossus.controller.Sink
import core._

import java.nio.ByteBuffer

trait MessageDecoder[T] {
  def decode(buffer: ByteBuffer, len: Int): Seq[T]
}

trait MessageEncoder[T] {
  def encode(t: T): ByteBuffer
}


sealed trait DecodedResult[+T]

object DecodedResult {

  case class Static[T](value : T) extends DecodedResult[T] //this is what is formerly the Some(Input) in a Codec
  case class Stream[T](value : T, s : Sink[DataBuffer]) extends DecodedResult[T]

  def static[T](value : Option[T]) : Option[DecodedResult[T]] = value.map(x => Static(x))

}

/**
 * A Codec is a stateful object for converting requests/responses to/from DataBuffers.
 * IMPORTANT - when decoding, a codec must be able to handle both partial
 * responses and multiple responses in a single DataBuffer.  This is why a
 * codec is stateful and returns a Seq[O]
 */
trait Codec[Output,Input] {

  def encode(out: Output): DataReader
  /**
   * Decode a single object from a bytestream.
   */
  def decode(data: DataBuffer): Option[DecodedResult[Input]]

  /** This is only needed for codecs where closing the connection signals the
   * end of a response (for example, a http response with no content-length and
   * non-chunked transfer encoding)
   */
  def endOfStream(): Option[DecodedResult[Input]] = None

  def decodeAll(data: DataBuffer)(onDecode : DecodedResult[Input] => Unit) { if (data.hasUnreadData){
    var done: Option[DecodedResult[Input]] = None
    do {
      done = decode(data)
      done.foreach{onDecode}
    } while (done.isDefined && data.hasUnreadData)
  }}

  //not used
  def mapInput[U](iMapper: Option[DecodedResult[Input]] => Option[DecodedResult[U]]): Codec[Output,U] = {
    val in = this
    new Codec[Output, U]{
      def encode(output: Output) = in.encode(output)
      def decode(data: DataBuffer) = iMapper(in.decode(data))
      def reset() {
        in.reset()
      }
    }
  }

  //not used
  def mapOutput[U](iMapper: U => Output): Codec[U,Input] = {
    val in = this
    new Codec[U, Input] {
      def encode(output: U) = in.encode(iMapper(output))
      def decode(data: DataBuffer) = in.decode(data)
      def reset() {
        in.reset()
      }
    }
  }

  def reset()
}

object Codec {

  type ServerCodec[Request,Response] = Codec[Response,Request]
  type ClientCodec[Request,Response] = Codec[Request, Response]

  type CodecFactory[I,O] = () => Codec[I,O]
  type ServerCodecFactory[Request,Response] = CodecFactory[Response, Request]
  type ClientCodecFactory[Request, Response] = CodecFactory[Request, Response]
}

