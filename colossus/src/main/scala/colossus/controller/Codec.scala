package colossus.controller

import colossus.core.{DataBuffer, DataOutBuffer}
import colossus.service.Protocol

trait Codec[E <: Encoding] {
  def decode(data: DataBuffer): Option[E#Input]
  def encode(message: E#Output, buffer: DataOutBuffer)

  def endOfStream(): Option[E#Input]

  def reset()

  def decodeAll(data: DataBuffer)(onDecode: E#Input => Unit) {
    if (data.hasUnreadData) {
      var done: Option[E#Input] = None
      do {
        done = decode(data)
        done.foreach { onDecode }
      } while (done.isDefined && data.hasUnreadData)
    }
  }
}

object Codec {
  type Server[P <: Protocol] = Codec[Encoding.Server[P]]
  type Client[P <: Protocol] = Codec[Encoding.Client[P]]
}
