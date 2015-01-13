package colossus.util.bson

import akka.util.{ByteString, ByteStringBuilder}

trait Writable {

  def encode(): ByteString

  def putCString(builder: ByteStringBuilder, value: String): ByteStringBuilder = {
    builder.putBytes(value.getBytes("utf-8")).putByte(0)
  }
}
