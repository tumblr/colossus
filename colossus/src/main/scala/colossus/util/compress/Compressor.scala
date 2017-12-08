package colossus.util.compress

import akka.util.ByteString

trait Compressor {
  def compress(bytes: ByteString): ByteString

  def decompress(bytes: ByteString): ByteString
}

object NoCompressor extends Compressor {
  def compress(bytes: ByteString): ByteString = bytes

  def decompress(bytes: ByteString): ByteString = bytes
}
