package colossus.protocols.mongodb.message

import java.nio.ByteOrder
import java.util.concurrent.atomic.AtomicInteger

import akka.util.ByteString
import colossus.util.bson.Writable

trait Message extends Writable {

  implicit val byteOrder = ByteOrder.LITTLE_ENDIAN

  val requestID: Int = RequestIDGenerator.generate

  def responseTo: Int

  def opCode: Int

  def encodeBody(): ByteString

  override def encode(): ByteString = {
    val body = encodeBody()

    ByteString.newBuilder
      .putInt(body.length + 16) // header length is always 16
      .putInt(requestID)
      .putInt(responseTo)
      .putInt(opCode)
      .append(body)
      .result()
  }
}
