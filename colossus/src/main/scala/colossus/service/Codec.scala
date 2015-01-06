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

/**
 * NOTE - for now the meta field here will be mutable for simplicity and to
 * avoid excessive object construction.  We could go instead with an implicit
 * wrapper class that inherits the Response trait
 *
 * Proposed change - for the most part all the response meta stuff is only
 * needed for post-processing of a response.  Perhaps the way to do this is
 * have a single callback whose return value is something like the
 * ResponseMeta.
 *
 * So far these are the things we certainly need to do in post-processing
 * - get request metric tags
 * - ability to disconnect client after write (for http 1.0) 
 *
 * Proposed new callback signature
 *
 * def onWrite: (I, O) => ResponseMeta
 *
 * Proposed new ResponseMeta fields
 * tags : TagMap
 * disconnect : Boolean
 * 
 * In fact, we don't even need to attach this to a response, DUH
 */


sealed trait OnWriteAction
object OnWriteAction {
  case object Disconnect extends OnWriteAction
  case object DoNothing extends OnWriteAction
}
import OnWriteAction._

case class Completion[+O](value: O, tags: TagMap = TagMap.Empty, onwrite: OnWriteAction = DoNothing) {
  def withTags(newtags: (String, String)*) = copy(tags = tags ++ newtags.toMap)
  def onWrite(w: OnWriteAction) = copy(onwrite = w)
}


object Completion {
  implicit def liftObj[O](obj: O): Completion[O] = Completion(obj)
}

sealed trait Response[+O]{self =>
  
  def withTags(newtags: (String, String)*): Response[O] = mapCompletion(_.withTags(newtags:_*))

  def mapCompletion[U >: O](f: Completion[O] => Completion[U])(implicit executionContext: ExecutionContext = ExecutionContext.global): Response[U]
}

object Response {
  case class SyncResponse[O](result: Completion[O]) extends Response[O] {
    def mapCompletion[U >: O](f: Completion[O] => Completion[U])(implicit executionContext: ExecutionContext = ExecutionContext.global)= SyncResponse(f(result))
  }

  case class AsyncResponse[O](result: Future[Completion[O]]) extends Response[O] {

    def mapCompletion[U >: O](f: Completion[O] => Completion[U])(implicit executionContext: ExecutionContext = ExecutionContext.global) = {
      AsyncResponse(result.map{c => f(c)})
    }
  }

  case class CallbackResponse[O](callback: Callback[Completion[O]]) extends Response[O]{
    def mapCompletion[U >: O](f: Completion[O] => Completion[U])(implicit executionContext: ExecutionContext = ExecutionContext.global) = {
      CallbackResponse(callback.map{c => f(c)})
    }

  }

  def complete[O](value: O): Completion[O] = new Completion(value)


  implicit def liftSync[O](o: O): Response[O] = SyncResponse(Completion(o))
  implicit def liftFuture[O](f: Future[O])(implicit ex: ExecutionContext): Response[O] = AsyncResponse(f.map{x => Completion(x)})
  implicit def liftCallback[O](c: Callback[O]): Response[O] = CallbackResponse(c.map{x => Completion(x)})
  implicit def liftCompletedSync[O](o: Completion[O]): Response[O] = SyncResponse(o)
  implicit def liftCompletedFuture[O](f: Future[Completion[O]]): Response[O] = AsyncResponse(f)
  implicit def liftCompletedCallback[O](c: Callback[Completion[O]]): Response[O] = CallbackResponse(c)
}
