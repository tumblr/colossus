package colossus
package service

import core._

import scala.concurrent.{ExecutionContext, Future}
import java.nio.ByteBuffer
import metrics._

trait MessageDecoder[T] {
  def decode(buffer: ByteBuffer, len: Int): Seq[T]
}

trait MessageEncoder[T] {
  def encode(t: T): ByteBuffer
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
  def decode(data: DataBuffer): Option[Input]

  def decodeAll(data: DataBuffer)(onDecode : Input => Unit) { if (data.hasUnreadData){
    var done: Option[Input] = None
    do {
      done = decode(data)
      done.foreach{onDecode}
    } while (done.isDefined && data.hasUnreadData)
  }}

  def mapInput[U](oMapper: Input => U): Codec[Output,U] = {
    val in = this
    new Codec[Output, U]{
      def encode(output: Output) = in.encode(output)
      def decode(data: DataBuffer) = in.decode(data).map{i => oMapper(i)}
      def reset() {
        in.reset()
      }
    }
  }
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

