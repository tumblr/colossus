package colossus.util.compress

import java.util.zip.{Deflater, Inflater}

import akka.util.{ByteString, ByteStringBuilder}

class ZCompressor(bufferKB: Int = 10) extends Compressor {
  val buffer = new Array[Byte](1024 * bufferKB)

  def compress(bytes: ByteString): ByteString = {
    val deflater = new Deflater
    deflater.setInput(bytes.toArray)
    deflater.finish()
    val builder = new ByteStringBuilder
    var numRead = 0
    do {
      numRead = deflater.deflate(buffer)
      builder.putBytes(buffer, 0, numRead)
    } while (numRead > 0)
    deflater.end()
    builder.result()
  }

  def decompress(bytes: ByteString): ByteString = {
    val inflater = new Inflater
    inflater.setInput(bytes.toArray)
    val builder = new ByteStringBuilder
    var numRead = 0
    do {
      numRead = inflater.inflate(buffer)
      builder.putBytes(buffer, 0, numRead)
    } while (numRead > 0)
    inflater.end()
    builder.result()
  }
}