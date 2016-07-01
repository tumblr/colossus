package colossus
package controller

import core.{DataBuffer, DataOutBuffer}

trait StaticCodec[E <: Encoding] {
  def decode(data: DataBuffer): Option[E#Input]
  def encode(message: E#Output, buffer: DataOutBuffer)

  def endOfStream(): Option[E#Input]

  def reset()

  def decodeAll(data: DataBuffer)(onDecode : E#Input => Unit) { if (data.hasUnreadData){
    var done: Option[E#Input] = None
    do {
      done = decode(data)
      done.foreach{onDecode}
    } while (done.isDefined && data.hasUnreadData)
  }}
}


object StaticCodec {

  import service.Protocol

  type Server[P <: Protocol] = StaticCodec[P#ServerEncoding]
  type Client[P <: Protocol] = StaticCodec[P#ClientEncoding]

}

