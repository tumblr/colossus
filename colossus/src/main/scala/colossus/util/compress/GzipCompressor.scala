package colossus.util.compress

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import akka.util.{ByteString, ByteStringBuilder}

class GzipCompressor(bufferKB: Int = 10) extends Compressor {
  override def compress(bytes: ByteString): ByteString = {
    val bos = new ByteArrayOutputStream(bytes.length)
    val gzip = new GZIPOutputStream(bos)
    gzip.write(bytes.toArray)
    gzip.close()
    val compressed = bos.toByteArray
    bos.close()
    ByteString(compressed)
  }

  override def decompress(bytes: ByteString): ByteString = {
    val inputStream = new GZIPInputStream(new ByteArrayInputStream(bytes.toArray))
    val buffer: Array[Byte] = new Array[Byte](1024 * bufferKB)
    var read: Int = 0
    val builder = new ByteStringBuilder
    read = inputStream.read(buffer)
    while (read > 0) {
      builder.putBytes(buffer, 0, read)
      read =inputStream.read(buffer)
    }

    inputStream.close()
    builder.result()
  }
}